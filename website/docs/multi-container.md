# Deploy a multi-container service

Some stateful service have multiple containers, typically having frontend, backend, and database in their own container.

XDN supports deployment of stateful service with multiple containers. Developers need to specify the properties of
the service, including the containers of that service. 
An example of that service properties declaration is shown below, in `wordpress.yaml` file.
```yaml
# wordpress.yaml
---
name: myblog
components:
   - wordpress:
        image: wordpress:6.5.4-fpm-alpine
        port: 80
        entry: true
        environments:
           - WORDPRESS_CONFIG_EXTRA:
                define('FORCE_SSL_ADMIN', false);
                define('FORCE_SSL_LOGIN', false);
   - database:
        image: mysql:8.4.0
        expose: 3306
        stateful: true
        environments:
           - MYSQL_ROOT_PASSWORD: supersecret
deterministic: false
state: database:/var/lib/mysql/
consistency: linearizability
```

Then, to deploy that multi-container service, use the following command:
``` bash
xdn launch myblog --file=wordpress.yaml
```
If successful, you wil see the following output.
```
Launching bookcatalog service with the following configuration:
  docker image  : wordpress:6.5.4-fpm-alpine,mysql:8.4.0
  http port     : 80
  consistency   : linearizable
  deterministic : false
  state dir     : database:/var/lib/mysql/

The service is successfully launched 🎉🚀
Access your service at the following permanent URL:
  > http://myblog.xdnapp.com/


Retrieve the service's replica locations with this command:
  xdn service info myblog
Destroy the replicated service with this command:
  xdn service destroy myblog
```

Limitations:

- XDN only supports at most one stateful container. Multiple stateful container requires snapshot transaction support,
  currently unimplemented.
- XDN only supports at most one entry container. Supporting multiple entry containers require developer to declare the
  ports of all those entry containers, making the specification more complex. Currently, this feature is not a priority
  for XDN.
