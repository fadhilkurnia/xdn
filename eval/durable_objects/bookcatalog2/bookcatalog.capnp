using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "main", worker = .mainWorker),
    (name = "DO_DIR", disk = (writable = true)),
  ],

  sockets = [
    ( name = "http",
      address = "*:9090",
      http = (),
      service = "main"
    ),
  ]
);

const mainWorker :Workerd.Worker = (
  modules = [ (name="worker", esModule = embed "bookcatalog.js") ],
  durableObjectNamespaces = [
    (className = "BookCatalog", uniqueKey="210bd0cbd803ef7883a1ee9d86cce06e", enableSql=true)
  ],
  durableObjectStorage = ( localDisk = "DO_DIR" ),
  bindings = [
    (name = "catalog", durableObjectNamespace = "BookCatalog"),
  ],
  compatibilityDate = "2024-09-03",
);
