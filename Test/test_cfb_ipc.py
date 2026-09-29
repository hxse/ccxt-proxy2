"""真实 Unix socket 验证取消、幂等和边界；终端执行由离线样本驱动。"""

import asyncio
import threading

import pytest

from src.cfb.client import Client
from src.cfb.config import BridgeConfig, Settings
from src.cfb.dispatch import Dispatcher
from src.cfb.errors import BridgeError
from src.cfb.ipc import MAX_RESPONSE, Request, Response, read_frame
from src.cfb.models import Operation
from src.cfb.results import Reply, SubmissionResult
from src.cfb.server import Server
from src.cfb.service import BridgeService
from src.cfb.worker import WorkerState

ORDER = {"is_live": False, "exchange_id": "DCE", "instrument_id": "m2701",
         "side": "buy", "offset": "open", "volume": 1, "price": 3514}


async def wait_for(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(.005)


def fixture(tmp_path):
    settings = Settings(bridge=BridgeConfig(data_dir=tmp_path))
    service = BridgeService(settings)
    dispatcher = Dispatcher(settings, service.runtime)
    service.dispatcher = dispatcher
    dispatcher.ready = True
    dispatcher.state = WorkerState(query_ready=True, trading_ready=True, ownership_clear=True)
    server = Server(service, tmp_path / "cfb.sock")
    client = Client(server.path)
    client.settings, client.enabled, client.timeout = settings, True, 2
    return service, dispatcher, server, client


def finish(dispatcher, job):
    reply = Reply(request_id=job.request_id, status=202,
                  body=SubmissionResult(request_id=job.request_id, submission_status="submitted",
                                        order_id="000123").model_dump(mode="json"))
    dispatcher._finish(job, reply)
    return reply


def test_reply_and_saved_idempotency_survive_readiness_loss(tmp_path):
    async def run():
        _, dispatcher, server, client = fixture(tmp_path)
        await server.start()
        operation = Operation(action="create_limit_order", parameters=ORDER)
        try:
            pending = asyncio.create_task(client.call("execute", request_id="cfb-first", operation=operation, key="one"))
            await wait_for(lambda: dispatcher.queue)
            job = dispatcher.queue.popleft()
            job.started = True
            expected = finish(dispatcher, job)
            result = await pending
            assert result.status == 202 and result.body == expected.body
            assert result.headers["X-Request-ID"] == "cfb-first"
            dispatcher.ready = False
            repeated = await client.call("execute", request_id="cfb-second", operation=operation, key="one")
            assert repeated.body == expected.body and repeated.headers["X-Request-ID"] == "cfb-first"
            assert not dispatcher.queue
            changed = await client.call("execute", operation=Operation(action=operation.action, parameters=ORDER | {"volume": 2}), key="one")
            assert changed.status == 409 and changed.body["error"]["code"] == "IDEMPOTENCY_CONFLICT"
        finally:
            await client.close()
            await server.close()
    asyncio.run(run())


@pytest.mark.parametrize("started", [False, True])
def test_disconnect_only_cancels_work_that_has_not_started(tmp_path, started):
    async def run():
        _, dispatcher, server, client = fixture(tmp_path)
        await server.start()
        operation = Operation(action="create_limit_order", parameters=ORDER)
        try:
            pending = asyncio.create_task(client.call("execute", request_id="cfb-cancel", operation=operation, key="cancel"))
            await wait_for(lambda: dispatcher.queue)
            job = dispatcher.queue[0]
            if started:
                dispatcher.queue.popleft()
                job.started = True
                dispatcher.journal.phase(job.request_id, "sending", effect="unknown")
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            await wait_for(lambda: job.cancelled.is_set())
            assert not job.future.cancelled()
            if started:
                assert not job.future.done()
                expected = finish(dispatcher, job)
                assert dispatcher.journal.lookup(operation, "cancel") == expected
            else:
                await wait_for(lambda: job.future.done())
                assert not dispatcher.queue
                saved = dispatcher.journal.lookup(operation, "cancel")
                assert saved.status == 499 and saved.body["submission_status"] is None
        finally:
            await client.close()
            await server.close()
    asyncio.run(run())


@pytest.mark.parametrize("payload", [b'{bad}\n', b'{"version":2,"kind":"health","request_id":"test"}\n', b'x' * 70000 + b'\n'])
def test_invalid_frames_never_enter_dispatcher(tmp_path, payload):
    async def run():
        _, dispatcher, server, client = fixture(tmp_path)
        await server.start()
        try:
            reader, writer = await asyncio.open_unix_connection(server.path, limit=MAX_RESPONSE)
            writer.write(payload)
            await writer.drain()
            async with asyncio.timeout(2):
                result = Response.model_validate_json(await read_frame(reader, MAX_RESPONSE))
            assert result.status == 422
            assert isinstance(result.body, dict)
            error = result.body["error"]
            assert isinstance(error, dict) and error["code"] == "INVALID_ARGUMENTS"
            assert not dispatcher.queue
            writer.close()
            await writer.wait_closed()
        finally:
            await client.close()
            await server.close()
    asyncio.run(run())


def test_wrong_configuration_cannot_send_to_a_restored_old_account(tmp_path):
    async def run():
        _, dispatcher, server, client = fixture(tmp_path)
        await server.start()
        client.settings = Settings()
        try:
            result = await client.call("execute", operation=Operation(action="create_limit_order", parameters=ORDER))
            assert result.status == 503 and result.body["submission_status"] is None
            assert not dispatcher.queue
        finally:
            await client.close()
            await server.close()
    asyncio.run(run())


def test_missing_worker_is_explicit_and_not_retried(tmp_path):
    async def run():
        client = Client(tmp_path / "absent.sock")
        client.enabled = True
        with pytest.raises(BridgeError) as error:
            await client.call("execute", operation=Operation(action="create_limit_order", parameters=ORDER))
        assert error.value.status == 503 and error.value.submission_status is None
        assert not client.writers
    asyncio.run(run())


def test_execution_identity_required_and_protocol_has_no_shell_command():
    with pytest.raises(ValueError):
        Request(kind="execute", request_id="test", operation=Operation(action="fetch_positions", parameters={"is_live": False, }))
    with pytest.raises(ValueError):
        Request.model_validate({"kind": "shell", "request_id": "test", "command": "anything"})
    with pytest.raises(ValueError, match="configuration identity"):
        Request(kind="ready", request_id="test")


def test_ready_checks_same_configuration_identity_as_execution(tmp_path):
    async def run():
        service, dispatcher, server, client = fixture(tmp_path)
        await server.start()
        try:
            matching = await client.call("ready")
            assert matching.status == 200 and matching.body["trading_ready"] is True
            client.settings = client.settings.model_copy(update={"vnc_enabled": False})
            for kind in ("status", "health"):
                assert (await client.call(kind)).status == 200
            ready = await client.call("ready", request_id="cfb-ready-mismatch")
            assert ready.status == 503
            assert ready.body["error"]["code"] == "SERVICE_NOT_READY"
            assert ready.headers["X-Request-ID"] == "cfb-ready-mismatch"
            assert not dispatcher.queue
            client.settings = service.settings
            assert (await client.call("ready")).status == 200
        finally:
            await client.close()
            await server.close()
    asyncio.run(run())


def test_disconnect_between_enqueue_and_admission_reply_removes_job(tmp_path, monkeypatch):
    release = threading.Event()

    async def run():
        _, dispatcher, server, client = fixture(tmp_path)
        original = dispatcher.submit

        def submit(*args):
            job = original(*args)
            assert release.wait(2)
            return job

        monkeypatch.setattr(dispatcher, 'submit', submit)
        await server.start()
        try:
            pending = asyncio.create_task(client.call('execute', request_id='cfb-admission', key='admission',
                operation=Operation(action='create_limit_order', parameters=ORDER)))
            await wait_for(lambda: dispatcher.queue)
            job = dispatcher.queue[0]
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            await wait_for(lambda: job.cancelled.is_set())
            release.set()
            await wait_for(lambda: job.future.done())
            assert not dispatcher.queue
            assert job.future.result().status == 499
        finally:
            release.set()
            await client.close()
            await server.close()
    asyncio.run(run())
