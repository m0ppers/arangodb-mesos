namespace arangodb {

  enum class ClusterState {
    CREATED,
    BOOTING,
    READY,
    CLOSING,
    SHUTDOWN,
    BROKEN
  };

  class DatabaseManager {
    public:
      void loadState ();

      Result createCluster (const string& name);
      Result bootCluster (const string& name);
      void shutdownCluster (const string& name);
      ClusterState clusterState (const string& name);
  };

}
