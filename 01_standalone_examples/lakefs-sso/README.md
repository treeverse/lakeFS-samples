# Running lakeFS with SSO locally

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This sample explain how to run lakeFS with local SSO (i.e no need for auth0 or azure entra). 
We will use [Keycloak](https://www.keycloak.org/) Docker image as our local SSO server - A.K.A Identity Provider (IdP).

## Prerequisites

- Docker installed on your local machine
- To use localhost (or 127.0.0.1) to communicate between two different containers (lakeFS and Keycloak), **you must use Host Network Mode in Docker**. In Docker’s default "Bridge" mode, each container has its own isolated network stack, so localhost only points to the container itself. By switching to Host mode, the containers share the host's IP address and network namespace.
  - To use Host Network Mode with Docker Compose, you must first enable a specific setting in Docker Desktop:

> > **Enable Host Networking in Docker Desktop**
> > Host networking is supported on **Docker Desktop version 4.34 and later**. 
> >
> > > 1. Open **Docker Desktop**.
> > > 2. Navigate to **Settings** (gear icon) > **Resources** > **Network**.
> > > 3. Check the box for **Enable host networking**.
> > > 4. Click **Apply & Restart**.

> > **Docker Compose File is already configured**
> >
> > > In the [docker-compose.yml](./docker-compose.yml) file of this sample, `network_mode` is set to `host` for both lakeFS and Keycloak services. Note that when using host mode, **port mappings (ports:) in Docker Compose are ignored** because the container uses the host's ports directly.

## Setup

1. Start by cloning this repository:
  ```bash
   git clone https://github.com/treeverse/lakeFS-samples
   cd lakeFS-samples/01_standalone_examples/lakefs-sso
  ```
2. Login to [Treeverse Dockerhub](https://hub.docker.com/u/treeverse) by using the granted token so lakeFS Enterprise proprietary image can be retrieved. [Contact Sales](https://lakefs.io/contact-sales/) to get the token and license file for lakeFS Enterprise:
  ```bash
   docker login -u externallakefs
  ```
3. Copy the lakeFS license file to `lakeFS-samples/01_standalone_examples/lakefs-sso` folder, then change lakeFS license file name and installation ID in the following command and run the command to provision the full stack which includes lakeFS Enterprise and Keycloak:
  ```bash
   LAKEFS_LICENSE_FILE_NAME=license-org-name-installation-id.token LAKEFS_INSTALLATION_ID=installation-id docker compose up
  ```

> Keycloak will run with preconfigured realm (`dev-realm`), client for `OIDC` and few users (see [realm-export.json](./realm-export.json)).

> **Using OIDC as SSO protocol:** This sample uses [lakefs-oidc.yaml](./lakefs-oidc.yaml) configuration file to run lakeFS with OIDC authentication.

## Demo Instructions

1. Open your browser and navigate to [http://localhost:8000](http://localhost:8000) and you should be redirected to Keycloak login page.

> **Use following preconfigured users in Keycloak (in [realm-export.json](./realm-export.json)) to login:**

> - User: `lakefs`, Password: `lakefs` - Has access to lakeFS (part of `lakeFS-Users` group in Keycloak)
> - User: `dev`, Password: `dev` - Also has access to lakeFS (part of `lakeFS-Users` group)
> - User: `guest`, Password: `guest` - No access to lakeFS (not part of `lakeFS-Users` group) can be used to test access denied scenario based lakeFS validate claims config.

1. If you want then you can access Keycloak admin page [http://localhost:8080/](http://localhost:8080/) (Username: `admin`, Password: `admin`)

## OPTIONAL: Stop the server

Note: This is useful to clean previous Keycloak volume data between different runs in case of changes in [realm-export.json](./realm-export.json).

```sh
docker compose down -v
```

