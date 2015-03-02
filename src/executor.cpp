////////////////////////////////////////////////////////////////////////////////
/// @brief ArangoDB Mesos Executor
///
/// @file
///
/// DISCLAIMER
///
/// Copyright 2015 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Dr. Frank Celler
/// @author Copyright 2015, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include <iostream>
#include <mutex>
#include <unordered_map>

#include <mesos/executor.hpp>
#include <mesos/resources.hpp>

#include <stout/duration.hpp>
#include <stout/os.hpp>

#include <unistd.h>

#include "logging/logging.hpp"

#include "utils.h"

using namespace std;
using namespace mesos;
using namespace arangodb;

// -----------------------------------------------------------------------------
// --SECTION--                                                external processes
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief maps taskId to PID
////////////////////////////////////////////////////////////////////////////////

mutex TaskId2PidLock;
unordered_map<string, pid_t> TaskId2Pid;

////////////////////////////////////////////////////////////////////////////////
/// @brief low level exec
////////////////////////////////////////////////////////////////////////////////

void ExecuteTask (const TaskInfo& task) {
  const string& executorId = task.executor().executor_id().value();
  const string& data = task.data();
  vector<string> args = split(data, '\n');

  Resources resources = task.resources();

  LOG(INFO)
  << "executor " << executorId
  << " with " << join(args, " ")
  << " and " << resources;

  Resources persistent = resources.filter(Resources::isPersistentVolume);

  for (const auto& volume : persistent) {
    const string& path = volume.disk().volume().container_path();

    LOG(INFO)
    << "persistent volume " << path;

    os::mkdir(path + "/data");
    os::mkdir(path + "/logs");
    os::mkdir(path + "/apps");
  }

  size_t len = data.size();
  char** a = static_cast<char**>(malloc(sizeof(char*) * (len + 1)));

  for (size_t i = 0;  i < len;  ++i) {
    a[i] = const_cast<char*>(args[i].c_str());
  }

  a[len] = nullptr;

  execv(args[0].c_str(), a);
   
  // upps, exec failed
  _exit(1);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief forks a new process
////////////////////////////////////////////////////////////////////////////////

struct ExternalInfo {
  pid_t pid;
  bool failed;
};

ExternalInfo StartExternalProcess (const TaskInfo& task) {
  ExternalInfo info = { 0, true };
  pid_t processPid;

  processPid = fork();

  // child process
  if (processPid == 0) {
    close(0);

#ifdef CLOSE_OUTPUT
    fcntl(1, F_SETFD, 0);
    fcntl(2, F_SETFD, 0);
#endif

    // ignore signals in worker process
    signal(SIGINT,  SIG_IGN);
    signal(SIGTERM, SIG_IGN);
    signal(SIGHUP,  SIG_IGN);
    signal(SIGUSR1, SIG_IGN);

    // execute worker
    ExecuteTask(task);
  }

  // parent
  if (processPid == -1) {
    cerr << "fork failed\n";

    info.failed = true;
    return info;
  }

  info.pid = processPid;
  info.failed = false;

  return info;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief runs a new process
////////////////////////////////////////////////////////////////////////////////

struct StartInfo {
  StartInfo (ExecutorDriver* driver, const TaskInfo& task) 
    : driver(driver), task(task) {
  }

    ExecutorDriver* driver;
    const TaskInfo task;
};

void* RunProcess (void* args) {
  StartInfo* info = static_cast<StartInfo*>(args);
  ExecutorDriver* driver = info->driver;
  const TaskInfo& task = info->task;

  ExternalInfo external = StartExternalProcess(task);

  {
    TaskStatus status;
    status.mutable_task_id()->CopyFrom(task.task_id());

    if (external.failed) {
      status.set_state(TASK_FAILED);
      driver->sendStatusUpdate(status);
      delete info;
      return nullptr;
    }

    cout << "PID " << external.pid << "\n";

    status.set_state(TASK_RUNNING);
    driver->sendStatusUpdate(status);
  }

  {
    lock_guard<mutex> lock(TaskId2PidLock);

    const string& taskId = task.task_id().value();

    TaskId2Pid[taskId] = external.pid;
  }

  TaskStatus status;
  status.mutable_task_id()->CopyFrom(task.task_id());

  int s;
  waitpid(external.pid, &s, WUNTRACED);

  cout << "WAIT for pid " << external.pid << " returned\n";

  if (WIFEXITED(s)) {
    if (WEXITSTATUS(s) == 0) {
      cout << "EXIT " << external.pid << " status == 0\n";
      status.set_state(TASK_FINISHED);
    }
    else {
      cout << "EXIT " << external.pid << " status != 0\n";
      status.set_state(TASK_FAILED);
    }
  }
  else if (WIFSIGNALED(s)) {
    cout << "EXIT " << external.pid << " signalled\n";
    status.set_state(TASK_FAILED);
  }
  else if (WIFSTOPPED(s)) {
    cout << "EXIT " << external.pid << " stopped\n";

    // TODO(fc) deal with stopped, but how?
    kill(external.pid, 9);
    status.set_state(TASK_FAILED);
  }

  driver->sendStatusUpdate(status);

  return nullptr;
}

// -----------------------------------------------------------------------------
// --SECTION--                                              class ArangoExecutor
// -----------------------------------------------------------------------------

class ArangoExecutor : public Executor {

// -----------------------------------------------------------------------------
// --SECTION--                                     constructors and desctructors
// -----------------------------------------------------------------------------

  public:

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

    virtual ~ArangoExecutor () {
    }

// -----------------------------------------------------------------------------
// --SECTION--                                                  Executor methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief registered
////////////////////////////////////////////////////////////////////////////////

    void registered (ExecutorDriver* driver,
                     const ExecutorInfo& executorInfo,
                     const FrameworkInfo& frameworkInfo,
                     const SlaveInfo& slaveInfo)  override {
      _executorId = executorInfo.executor_id().value();

      cout
      << "Registered executor " << _executorId << " on "
      << slaveInfo.hostname()
      << endl;

      string data;
      slaveInfo.SerializeToString(&data);

      driver->sendFrameworkMessage(data);
    }

////////////////////////////////////////////////////////////////////////////////
/// @brief reregistered
////////////////////////////////////////////////////////////////////////////////

    void reregistered (ExecutorDriver* driver,
                       const SlaveInfo& slaveInfo) override {
      cout << "Re-registered executor on " << slaveInfo.hostname() << endl;
    }

////////////////////////////////////////////////////////////////////////////////
/// @brief disconnected
////////////////////////////////////////////////////////////////////////////////

    void disconnected (ExecutorDriver* driver) override {
      cout << "disconnected\n";
    }

////////////////////////////////////////////////////////////////////////////////
/// @brief launchTask
////////////////////////////////////////////////////////////////////////////////

    void launchTask (ExecutorDriver* driver, const TaskInfo& task) override {
      cout << "Starting task " << task.task_id().value() << endl;

      TaskStatus status;
      status.mutable_task_id()->MergeFrom(task.task_id());

      StartInfo* info = new StartInfo(driver, task);

      pthread_t pthread;
      int res = pthread_create(&pthread, NULL, &RunProcess, info);

      if (res != 0) {
        status.set_state(TASK_FAILED);
        delete info;
      } 
      else {
        pthread_detach(pthread);
        status.set_state(TASK_RUNNING);
      }

      driver->sendStatusUpdate(status);
    }

////////////////////////////////////////////////////////////////////////////////
/// @brief killTask
////////////////////////////////////////////////////////////////////////////////

    void killTask (ExecutorDriver* driver, const TaskID& taskId) override {
      const string& ti = taskId.value();
      pid_t pid;

      {
        lock_guard<mutex> lock(TaskId2PidLock);

        auto iter = TaskId2Pid.find(ti);

        if (iter == TaskId2Pid.end()) {
          LOG(WARNING)
          << "unknown task id '" << ti << "'";
          return;
        }

        pid = iter->second;
      }

      // TODO(fc) be graceful
      kill(pid, 9);
    }

////////////////////////////////////////////////////////////////////////////////
/// @brief frameworkMessage
////////////////////////////////////////////////////////////////////////////////

    void frameworkMessage (ExecutorDriver* driver,
                           const string& data) override {
      cout << "frameworkMessage\n";
    }

////////////////////////////////////////////////////////////////////////////////
/// @brief shutdown
////////////////////////////////////////////////////////////////////////////////

    virtual void shutdown (ExecutorDriver* driver) override {
      cout << "shutdown\n";
    }

////////////////////////////////////////////////////////////////////////////////
/// @brief error
////////////////////////////////////////////////////////////////////////////////

    virtual void error(ExecutorDriver* driver, const string& message) override {
      cout << "error: " << message << "\n";
    }

// -----------------------------------------------------------------------------
// --SECTION--                                                 private variables
// -----------------------------------------------------------------------------

  private:

////////////////////////////////////////////////////////////////////////////////
/// @brief executor identifier
///
/// for example: arangodb:agency
////////////////////////////////////////////////////////////////////////////////

    string _executorId;
};

// -----------------------------------------------------------------------------
// --SECTION--                                                  public functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief main
////////////////////////////////////////////////////////////////////////////////

int main (int argc, char** argv)
{
  ArangoExecutor executor;
  MesosExecutorDriver driver(&executor);
  return driver.run() == DRIVER_STOPPED ? 0 : 1;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
