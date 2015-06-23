

namespace arangodb {

  enum class ResourcesState {
    IN_PROGRESS,
    READY,
    GONE
  };

  class Resources {
    public:
      double _cpus;
      size_t _memory;
      size_t _diskspace;
  };

  class ResourceManager {
    public:
      string locateResources (const Resources& min, const Resources& opt);
      string locateResources (const string& instanceId, const Resources& min, const Resources& opt);
      ResourcesState resourcesState (const string& resourcesId);
  };

}
