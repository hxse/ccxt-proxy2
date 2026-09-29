"""旧布局拆分保存原记录和会话；失败不得覆盖或用空库继续。"""

import fcntl
import sqlite3

import pytest

from src.cfb.config import AccountConfig, AccountsConfig, BridgeConfig, Settings
from src.cfb.journal import Journal
from src.cfb.migration import MigrationError, migrate
from src.cfb.models import Operation


def settings(root, mode):
    return Settings(bridge=BridgeConfig(data_dir=root, mode=mode),
                    accounts=AccountsConfig(live=AccountConfig(broker_id='6020', site='一套')))


def legacy(root):
    for mode, prefix in (('sandbox', 'simnow'), ('live', 'live')):
        session = root / 'sessions' / f'{prefix}-offline'
        session.mkdir(parents=True)
        (session / 'terminal.ini').write_text(mode)
        (session / 'drive').symlink_to('/wine-drive')
        journal = Journal(settings(root, mode))
        order = Operation(action='create_limit_order', parameters={'is_live': mode == 'live',
            'exchange_id': 'DCE', 'instrument_id': 'm2701', 'side': 'buy', 'offset': 'open',
            'volume': 1, 'price': 3000})
        journal.admit(f'cfb-{mode}', order, 'same-key')
        journal.phase(f'cfb-{mode}', 'sending', effect='unknown')
    (root / 'artifacts/op-evidence').mkdir(parents=True)
    (root / 'artifacts/op-evidence/receipt.csv').write_bytes(b'protected evidence')
    (root / 'logs').mkdir()
    (root / 'logs/old.log').write_bytes(b'audit history')


def test_split_keeps_journal_namespace_and_unknown_evidence(tmp_path):
    root = tmp_path / 'cfb'
    legacy(root)
    original = (root / 'state/operations.sqlite3').read_bytes()
    assert migrate(root)
    assert (tmp_path / 'cfb-legacy/state/operations.sqlite3').read_bytes() == original
    assert (tmp_path / 'cfb-legacy/logs/old.log').read_bytes() == b'audit history'
    for mode, prefix in (('sandbox', 'simnow'), ('live', 'live')):
        destination = root / mode
        assert (destination / 'state/operations.sqlite3').read_bytes() == original
        assert [p.name for p in (destination / 'sessions').iterdir()] == [f'{prefix}-offline']
        assert (destination / 'sessions' / f'{prefix}-offline/drive').is_symlink()
        assert (destination / 'artifacts/op-evidence/receipt.csv').read_bytes() == b'protected evidence'
        journal = Journal(settings(destination, mode))
        assert journal.unresolved() == 1
        with sqlite3.connect(journal.path) as database:
            assert database.execute('SELECT request_id FROM operations WHERE namespace=?',
                                    (journal.namespace,)).fetchall() == [(f'cfb-{mode}',)]
    assert not migrate(root)


@pytest.mark.parametrize('failure', ['mixed', 'backup', 'session', 'copy', 'lock'])
def test_migration_failures_preserve_old_data(tmp_path, monkeypatch, failure):
    root = tmp_path / 'cfb'
    legacy(root)
    original = (root / 'state/operations.sqlite3').read_bytes()
    lock = None
    if failure == 'mixed':
        (root / 'live').mkdir()
    elif failure == 'backup':
        (tmp_path / 'cfb-legacy').mkdir()
    elif failure == 'session':
        (root / 'sessions/unknown').mkdir()
    elif failure == 'copy':
        def fail(*args):
            raise OSError('no space')
        monkeypatch.setattr('src.cfb.migration._copy', fail)
    else:
        lock = (root / '.session.lock').open('a')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises((MigrationError, OSError)):
            migrate(root)
    finally:
        if lock:
            lock.close()
    assert (root / 'state/operations.sqlite3').read_bytes() == original
    assert (root / 'sessions/simnow-offline/terminal.ini').read_text() == 'sandbox'
    assert not (tmp_path / '.cfb-dual-staging').exists()


def test_interrupted_switch_finishes_only_a_completed_staging_tree(tmp_path):
    backup = tmp_path / 'cfb-legacy'
    backup.mkdir()
    root = tmp_path / 'cfb'
    with pytest.raises(MigrationError, match='未完成'):
        migrate(root)
    staged = tmp_path / '.cfb-dual-staging'
    staged.mkdir()
    (staged / '.ready').write_text('cfb-mode-layout-v1\n')
    (staged / 'sandbox').mkdir()
    (staged / 'live').mkdir()
    assert migrate(root)
    assert backup.is_dir() and (root / 'sandbox').is_dir() and (root / 'live').is_dir()
