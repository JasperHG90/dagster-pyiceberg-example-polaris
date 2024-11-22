"""
!!! Important: you need to run this before running the dagster example

!!! Important: after booting up the devcontainer, you need to bootstrap the
 postgres backend. If you don't do this, you will see the following output:

```
<html>
<head>
<meta http-equiv="Content-Type" content="text/html;charset=ISO-8859-1"/>
<title>Error 500 java.lang.IllegalStateException: Realm is not bootstrapped, please run server in bootstrap mode.</title>
</head>
<body><h2>HTTP ERROR 500 java.lang.IllegalStateException: Realm is not bootstrapped, please run server in bootstrap mode.</h2>
<table>
<tr><th>URI:</th><td>/api/management/v1/catalogs</td></tr>
<tr><th>STATUS:</th><td>500</td></tr>
<tr><th>MESSAGE:</th><td>java.lang.IllegalStateException: Realm is not bootstrapped, please run server in bootstrap mode.</td></tr>
<tr><th>SERVLET:</th><td>jersey</td></tr>
<tr><th>CAUSED BY:</th><td>java.lang.IllegalStateException: Realm is not bootstrapped, please run server in bootstrap mode.</td></tr>
</table>

</body>
</html>
```

Sets up the Polaris catalog by:

- Craeting a catalog
- Creating a principal with appropriate roles and permissions
- Creating a namespace in the catalog
- Writes a pyiceberg config file to /home/vscode/workspace/.pyiceberg.yaml

Run using:
"""

import enum
import json
import logging
import pathlib as plb
from urllib.parse import urljoin

import httpx
import yaml
from pydantic import BaseModel, ConfigDict, Field
from pyiceberg.catalog.rest import RestCatalog

CATALOG_NAME = "dagster_example_catalog"
NAMESPACE_NAME = "air_quality"
PRINCIPAL_NAME = "dagster"
PRINCIPAL_ROLE_NAME = "dagster_principal_role"
CATALOG_ROLE_NAME = "orchestration_catalog_role"


logger = logging.getLogger("polaris_setup")
handler = logging.StreamHandler()
format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(format)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class CatalogTypeEnum(enum.Enum):
    internal = "INTERNAL"
    external = "EXTERNAL"


class StorageTypeEnum(enum.Enum):
    s3 = "S3"
    gcs = "GCS"
    azure = "AZURE"
    file = "FILE"


class CatalogProperties(BaseModel):
    default_base_location: str = Field(alias="default-base-location")

    model_config = ConfigDict(
        extra="allow",
    )


class StorageConfigInfo(BaseModel):
    storageType: StorageTypeEnum

    class Config:
        use_enum_values = True


class CreateCatalogRequest(BaseModel):
    type: CatalogTypeEnum
    name: str
    properties: CatalogProperties
    storageConfigInfo: StorageConfigInfo

    class Config:
        use_enum_values = True


class PolarisManagementServiceClient:

    def __init__(self, url: str):
        self._logger = logging.getLogger("polaris_setup.PolarisManagementServiceClient")
        self._url = url
        self._client = httpx.Client(
            headers={
                "Authorization": "Bearer principal:root;realm:default-realm",
                "Content-Type": "application/json",
            }
        )
        self._logger.debug("Url: %s", self._url)
        self._logger.debug("Headers: %s", self._client.headers)

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        self._logger.debug("GET %s; params %s" % (urljoin(self._url, endpoint), params))
        resp = self._client.get(url=urljoin(self._url, endpoint), params=params)
        resp.raise_for_status()
        return resp.json()

    def _post(
        self, endpoint: str, params: dict | None = None, data: dict | str | None = None
    ) -> str:
        self._logger.debug(
            "POST %s; params %s; data %s" % (urljoin(self._url, endpoint), params, data)
        )
        resp = self._client.post(
            url=urljoin(self._url, endpoint), params=params, data=data
        )
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(e.response.text)
            raise e
        return resp.text

    def _put(
        self, endpoint: str, params: dict | None = None, data: dict | str | None = None
    ) -> str:
        self._logger.debug(
            "PUT %s; params %s; data %s" % (urljoin(self._url, endpoint), params, data)
        )
        resp = self._client.put(
            url=urljoin(self._url, endpoint), params=params, data=data
        )
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(e.response.text)
            raise e
        return resp.text

    def list_catalogs(self) -> dict:
        return self._get("catalogs")

    def create_catalog(self, catalog_conf: CreateCatalogRequest) -> str:
        return self._post(
            "catalogs",
            data=json.dumps({"catalog": catalog_conf.model_dump(by_alias=True)}),
        )

    def create_principal(self, principal_name: str) -> str:
        return self._post(
            "principals", data=json.dumps({"principal": {"name": principal_name}})
        )

    def create_principal_role(self, role_name: str) -> str:
        return self._post(
            "principal-roles", data=json.dumps({"principalRole": {"name": role_name}})
        )

    def create_catalog_role(self, catalog_name: str, role_name: str) -> str:
        return self._post(
            f"catalogs/{catalog_name}/catalog-roles",
            data=json.dumps({"catalogRole": {"name": role_name}}),
        )

    def assign_role_to_principal(self, principal_name: str, role_name: str) -> str:
        return self._put(
            f"principals/{principal_name}/principal-roles",
            data=json.dumps({"principalRole": {"name": role_name}}),
        )

    def assign_catalog_role_to_principal_role(
        self, catalog_name: str, principal_role_name: str, catalog_role_name: str
    ) -> str:
        return self._put(
            f"principal-roles/{principal_role_name}/catalog-roles/{catalog_name}",
            data=json.dumps({"catalogRole": {"name": catalog_role_name}}),
        )

    def grant_privilege_to_catalog_role(
        self, catalog_name: str, catalog_role_name: str, privilege: str
    ) -> str:
        return self._put(
            f"catalogs/{catalog_name}/catalog-roles/{catalog_role_name}/grants",
            data=json.dumps({"grant": {"type": "catalog", "privilege": privilege}}),
        )

    def list_principals(self) -> dict:
        return self._get("principals")

    def list_principal_roles(self) -> dict:
        return self._get("principal-roles")

    def list_catalog_roles(self, catalog_name: str) -> dict:
        return self._get(f"catalogs/{catalog_name}/catalog-roles")

    def get_principal(self, principal_name: str) -> dict:
        return self._get(f"principals/{principal_name}")

    def get_principal_roles(self, principal_name: str) -> dict:
        return self._get(f"principals/{principal_name}/principal-roles")

    def get_catalog_role_assigned_to_principal_role(
        self, principal_role_name: str, catalog_name: str
    ) -> dict:
        return self._get(
            f"principal-roles/{principal_role_name}/catalog-roles/{catalog_name}"
        )

    def get_catalog_role_grants(
        self, catalog_name: str, catalog_role_name: str
    ) -> dict:
        return self._get(
            f"catalogs/{catalog_name}/catalog-roles/{catalog_role_name}/grants"
        )


def create_catalog(client: PolarisManagementServiceClient):
    conf = CreateCatalogRequest(
        type=CatalogTypeEnum.internal,
        name=CATALOG_NAME,
        properties=CatalogProperties.model_validate(
            {"default-base-location": f"file:///tmp/{CATALOG_NAME}"}
        ),
        storageConfigInfo=StorageConfigInfo(storageType=StorageTypeEnum.file),
    )
    client.create_catalog(catalog_conf=conf)


def create_namespace_in_catalog(credentials: dict):
    catalog = RestCatalog(
        name=CATALOG_NAME,
        **{
            "uri": "http://polaris:8181/api/catalog",
            "credential": "%s:%s"
            % (
                credentials["credentials"]["clientId"],
                credentials["credentials"]["clientSecret"],
            ),
            "py-io-impl": " pyiceberg.io.fsspec.FsspecFileIO",
            "warehouse": "dagster_example_catalog",
            "scope": "PRINCIPAL_ROLE:ALL",
            "oauth2-server-uri": "http://polaris:8181/api/catalog/v1/oauth/tokens",
        },
    )
    logger.info("Connected to catalog %s", CATALOG_NAME)
    logger.info("Creating namespace %s", NAMESPACE_NAME)
    catalog.create_namespace_if_not_exists(NAMESPACE_NAME)
    logger.info("Namespace %s created", NAMESPACE_NAME)


def write_pyiceberg_config(credentials: dict):
    config_out = {
        "catalog": {
            CATALOG_NAME: {
                "uri": "http://polaris:8181/api/catalog",
                "credential": "%s:%s"
                % (
                    credentials["credentials"]["clientId"],
                    credentials["credentials"]["clientSecret"],
                ),
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
                "warehouse": CATALOG_NAME,
                "scope": "PRINCIPAL_ROLE:ALL",
                "oauth2-server-uri": "http://polaris:8181/api/catalog/v1/oauth/tokens",
            }
        }
    }

    logger.info("Writing pyiceberg config to /home/vscode/workspace/.pyiceberg.yaml")
    with plb.Path("/home/vscode/workspace/.pyiceberg.yaml").open("w") as outFile:
        yaml.safe_dump(config_out, outFile)


if __name__ == "__main__":
    client = PolarisManagementServiceClient(
        url="http://polaris:8181/api/management/v1/"
    )
    create_catalog(client=client)
    credentials = json.loads(client.create_principal(principal_name=PRINCIPAL_NAME))
    client.create_principal_role(role_name=PRINCIPAL_ROLE_NAME)
    client.create_catalog_role(catalog_name=CATALOG_NAME, role_name=CATALOG_ROLE_NAME)
    client.assign_role_to_principal(
        principal_name=PRINCIPAL_NAME, role_name=PRINCIPAL_ROLE_NAME
    )
    client.assign_catalog_role_to_principal_role(
        catalog_name=CATALOG_NAME,
        principal_role_name=PRINCIPAL_ROLE_NAME,
        catalog_role_name=CATALOG_ROLE_NAME,
    )
    client.grant_privilege_to_catalog_role(
        catalog_name=CATALOG_NAME,
        catalog_role_name=CATALOG_ROLE_NAME,
        privilege="CATALOG_MANAGE_CONTENT",
    )
    create_namespace_in_catalog(credentials=credentials)
    write_pyiceberg_config(credentials=credentials)
    logger.info("Polaris setup complete")
