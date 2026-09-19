// Exercise the vendored queue without loading the trading SDK or collecting data.
#include "vnctp.h"
#include <future>
#include <stdexcept>

struct Counts
{
    atomic<int> data_created{0}, data_destroyed{0};
    atomic<int> error_created{0}, error_destroyed{0};
};

struct Data
{
    Counts &counts;
    int value = 42;
    explicit Data(Counts &c) : counts(c) { ++counts.data_created; }
    ~Data() { ++counts.data_destroyed; }
};

struct Error
{
    Counts &counts;
    explicit Error(Counts &c) : counts(c) { ++counts.error_created; }
    ~Error() { ++counts.error_destroyed; }
};

Task make_task(Counts &counts)
{
    Task task{};
    task.task_data = make_shared<Data>(counts);
    task.task_error = make_shared<Error>(counts);
    return task;
}

void require(bool condition, const char *message)
{
    if (!condition)
        throw runtime_error(message);
}

void verify_queue(const string &scenario)
{
    Counts counts;
    if (scenario == "pending" || scenario == "consumed" || scenario == "destructor")
    {
        TaskQueue queue;
        for (int i = 0; i < 200; ++i)
            queue.push(make_task(counts));
        require(counts.data_destroyed == 0, "producer destruction freed queued data");
        if (scenario == "pending")
        {
            queue.terminate();
            require(counts.data_destroyed == 200, "pending data was not freed at termination");
            require(counts.error_destroyed == 200, "pending errors were not freed at termination");
            queue.terminate();
        }
        else if (scenario == "consumed")
        {
            Task consumer = queue.pop();
            queue.terminate();
            require(counts.data_destroyed == 199, "termination freed an in-flight task");
            require(static_cast<Data *>(consumer.task_data.get())->value == 42,
                    "in-flight payload is no longer readable");
            // This task remains alive until the consumer returns (or throws).
        }
        // Without terminate(), the queue destructor must also release pending tasks.
    }
    else if (scenario == "late")
    {
        TaskQueue queue;
        queue.terminate();
        for (int i = 0; i < 200; ++i)
        {
            {
                Task producer = make_task(counts);
                queue.push(producer);
                require(counts.data_destroyed == i, "push freed the producer's task");
            }
            require(counts.data_destroyed == i + 1, "terminated queue retained late data");
            require(counts.error_destroyed == i + 1, "terminated queue retained late errors");
        }
    }
    else if (scenario == "concurrent")
    {
        for (int run = 0; run < 16; ++run)
        {
            TaskQueue queue;
            promise<void> started;
            auto ready = started.get_future();
            thread producer([&]() {
                for (int i = 0; i < 500; ++i)
                {
                    queue.push(make_task(counts));
                    if (i == 0)
                        started.set_value();
                }
            });
            ready.wait();
            queue.terminate();
            producer.join();
            require(counts.data_created == counts.data_destroyed,
                    "push racing with termination leaked data");
            require(counts.error_created == counts.error_destroyed,
                    "push racing with termination leaked errors");
        }
    }
    else if (scenario == "waiting")
    {
        TaskQueue queue;
        promise<void> started;
        auto ready = started.get_future();
        atomic<bool> stopped{false};
        thread consumer([&]() {
            started.set_value();
            try { queue.pop(); }
            catch (const TerminatedError &) { stopped = true; }
        });
        ready.wait();
        queue.terminate();
        consumer.join();
        require(stopped, "termination did not wake the empty queue's consumer");
    }
    else
        throw runtime_error("unknown test scenario");

    require(counts.data_created == counts.data_destroyed, "data was leaked or freed twice");
    require(counts.error_created == counts.error_destroyed, "error was leaked or freed twice");
}

PYBIND11_MODULE(_ctp_queue_probe, m)
{
    m.def("verify", &verify_queue);
}
