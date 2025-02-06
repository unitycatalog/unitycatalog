# Unity Catalog UI

The Unity Catalog UI allows you to interact with a Unity Catalog server to view or create data and AI assets.

![UC UI](../assets/images/ui/uc-ui-expanded.png)

## Start Unity Catalog UI locally

To use the Unity Catalog UI, start a new terminal and ensure you have already started the UC server
(e.g., `./bin/start-uc-server`)

!!! warning "Prerequisites"
    The Unity Catalog UI requires both [Node](https://nodejs.org/en/download/package-manager) and
    [Yarn](https://classic.yarnpkg.com/lang/en/docs/install).

To start the UI locally, run the following commands to start `yarn`

```sh
cd /ui
yarn install
yarn start
```

## Assets

### Catalogs

The following steps show how you can create, describe, and delete UC catalogs.

=== "1. List and create catalogs"

    After clicking the top **Catalog** button, you will see your list of catalogs.  To create a catalog, click the
    **Create Catalog** button to the right.

![Unity Catalog Creation](../assets/images/ui/uc-ui-catalog-create-1.png)
    

=== "2. Create catalog dialog"

    Specify the name and include any comments when creating your catalog.

![Unity Catalog Catalog Name Writing](../assets/images/ui/uc-ui-catalog-create-2.png)

=== "3. Delete a catalog"

    Click the *horizontal three dots* next to **Create Schema** button to delete your catalog.

![Deleting the Catalog](../assets/images/ui/uc-ui-catalog-delete.png)

---

### Schemas

The following steps show how you can create, describe, and delete UC schemas.

=== "1. List and create schemas"

    After clicking on any catalog, the main dialog contains the list of available schemas.  Click the
    **Create Schemas** button to the right to create a new schema.

![Unity catalog Schema Creation](../assets/images/ui/uc-ui-schema-create-1.png)
    

=== "2. Create schemas dialog"

    Specify the name and include any comments when creating your schema.

![Naming the Schema](../assets/images/ui/uc-ui-schema-create-2.png)

=== "3. Delete a schema"

    Click the *horizontal three dots* to the right to delete your schema.

![Deleting the Schema](../assets/images/ui/uc-ui-schema-delete.png)

---

### Tables

The following steps show how you can view your UC table metadata and descriptions.

=== "1. View tables in schema (1)"

    Click on the schema (e.g., `unity.demo`) to view its tables.

![Showing Tables in Schema](../assets/images/ui/uc-ui-tables-1.png)
    
=== "2. View tables in schema (2)"

    Click on the schema (e.g., `unity.default`) to view its tables.

![Showing Tables](../assets/images/ui/uc-ui-tables-2.png)

=== "3. View table metadata"

    Click on any table (e.g., `unity.default.marksheet`) to view its metadata.  You also have the option to delete the
    table via the *three horizontal dots* on the right.

![Showing Tables dataTypes](../assets/images/ui/uc-ui-tables-3.png)

---

### Volumes

The following steps show how you can view your UC volume metadata and descriptions.

=== "1. Traverse to volumes"

    Using the left-hand nav bar, click on *catalog > schema* (e.g., `unity` > `default`) to view the available volumes.

![Checking Volume](../assets/images/ui/uc-ui-volumes-1.png)
    
=== "2. View volume metadata"

    Click on the volume (e.g., `unity.default.txt_files`) to view its metadata.  You have the option to delete it by
    click on the *three horizontal dots* to the right.

![View Volume Metadata](../assets/images/ui/uc-ui-volumes-2.png)

=== "3. Edit volume description"

    Click the edit button to change its descripton.

![Editing the Volume Description](../assets/images/ui/uc-ui-volumes-3.png)

---

### Functions

The following steps show how you can view your UC functions metadata and descriptions.

=== "1. Traverse to functions"

    Using the left-hand nav bar, click on *catalog > schema* (e.g., `unity` > `default`) to view the available
    functions.

![Showing Functions in Unity Catalog](../assets/images/ui/uc-ui-functions-1.png)
    
=== "2. View functions metadata"

    Click on the volume (e.g., `unity.default.lowercase`) to view its metadata.  You have the option to delete it by
    click on the *three horizontal dots* to the right.

![View Functions Metadata](../assets/images/ui/uc-ui-functions-2.png)

---

### Models

The following steps show how you can list, describe and delete your UC models.

=== "1. List and create models"

    After clicking on any *schema > models*, the main dialog contains the list of available models.  Click the
    **horizontal three dots** button to the right to create a new model.

![Listing Models](../assets/images/ui/uc-ui-models-1.png)

=== "2. Create model"

    Click the edit button to change its descripton.

![Creation of Model](../assets/images/ui/uc-ui-model-create-1.png)

=== "3. Create model dialog"

    Specify the name and include any comments when creating your model.

![Naming the Model and Its Comments](../assets/images/ui/uc-ui-model-create-2.png)

=== "4. Edit model description"

    Click the edit button to change its descripton.

![Editing the Model Description](../assets/images/ui/uc-ui-model-edit.png)

=== "5. Delete a model"

    Click the *horizontal three dots* to the right to delete your model.

![Deleting the Unity Catalog Model](../assets/images/ui/uc-ui-model-delete.png)

---

### Model Versions

The following steps show how you can view your UC model versions and their metadata.

=== "1. Traverse to models"

    Using the left-hand nav bar, click on *catalog > schema* (e.g., `unity` > `default`) to view the available models.

![Showing Models and Its creations Date](../assets/images/ui/uc-ui-models-1.png)

=== "2. View model versions"

    Click on the model (e.g., `unity.default.iris`) to view its version(s).

![Showing Model Version](../assets/images/ui/uc-ui-models-2.png)

=== "3. View model version metadata"

    Click on the model version to view its details.

![Show model Version Metadata](../assets/images/ui/uc-ui-models-3.png)

=== "4. Edit model version description"

    Click the edit button to change its descripton.

![Editing Version Description](../assets/images/ui/uc-ui-model-version-edit.png)

=== "5. Delete a model version"

    Click the *horizontal three dots* to the right to delete your model version.

![Deleting the Model Version](../assets/images/ui/uc-ui-model-version-delete.png)

---
## Conclusion

The Unity Catalog UI provides a comprehensive interface for managing data and AI assets. By following the steps outlined in this guide, you can effectively create, describe, and delete catalogs, schemas, tables, volumes, functions, models, and model versions. Ensure that you have the necessary prerequisites installed and follow the provided commands to start the UI locally. This documentation aims to help you navigate and utilize the Unity Catalog UI efficiently.