void DatabaseManagerImpl::startMore (ClusterInfo& info, Aspect& aspect) {
  !CHECK_KNOWN_RESOURCES!;
  other !START_NEW_RESOURCE!;
}



void DatabaseManagerImpl::bootingCluster (ClusterInfo& info) {
  for (auto& aspect : _aspects) {
    if (! agency.minimumReached(info)) {
      startMore(info, aspect);
      return;
    }
  }

  for (auto& aspect : _aspects) {
    if (! agency.maximumReached(info)) {
      startMore(info, aspect);
      return;
    }
  }
}


void DatabaseManagerImpl::checkClusterState (ClusterInfo& info) {
  switch (info.state()) {
    case ClusterState::CREATED:
    case ClusterState::READY:
    case ClusterState::BROKEN:
    case ClusterState::SHUTDOWN:
      return;

    case ClusterState::BOOTING:
      bootingCluster(info);
      return;

    case ClusterState::CLOSING:
      closingCluster(info);
      return;
  }
}



void DatabaseManagerImpl::mainLoop () {
  while (_active) {
    !LOCK!;

    for (const auto& iter : _clusters) {
      checkClusterState(iter.second);
    }

    !SLEEP!;
  }
}



Result DatabaseManager::createCluser (const string& name) {
  !LOCK!;

  Result res = checkName(name);

  if (res.isError()) {
    return res;
  }

  impl->_clusters[name] = _aspectManager->createClusterInfo(name);

  return Result::noError();
}



Result DatabaseManager::bootCluster (const string& name) {
  !LOCK!;

  auto iter = impl->_clusters.find(name);

  if (iter == impl->_clusters.end()) {
    return Result::error("unknown cluster '" + name + "'");
  }

  auto& cluster = iter->second;

  switch (cluster.state()) {
    case ClusterState::BROKEN:
      return Result::error("cluster '" + name + "' is broken, please shut-down");

    case ClusterState::BOOTING:
    case ClusterState::READY:
      return Result::noError();

    case ClusterState::CREATED:
    case ClusterState::CLOSING:
    case ClusterState::SHUTDOWN:
      cluster.setState(ClusterState::BOOTING);
      return Result::noError();
  }

  return Result::internalError();
}



Result DatabaseManager::shutdownCluster (const string& name) {
  !LOCK!;

  auto iter = impl->_clusters.find(name);

  if (iter == impl->_clusters.end()) {
    return Result::error("unknown cluster '" + name + "'");
  }

  auto& cluster = iter->second;

  switch (cluster.state()) {
    case ClusterState::CLOSING:
    case ClusterState::SHUTDOWN:
      return Result::noError();

    case ClusterState::CREATED:
    case ClusterState::BOOTING:
    case ClusterState::READY:
    case ClusterState::BROKEN:
      cluster.setState(ClusterState::CLOSING);
      return Result::noError();
  }

  return Result::internalError();
}



ClusterState DatabaseManager::clusterState (const string& name) {
  !LOCK!;

  auto iter = impl->_clusters.find(name);

  if (iter == impl->_clusters.end()) {
    return ClusterState::BROKEN;
  }

  return iter->second.state();
}
