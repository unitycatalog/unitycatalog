# Unity Catalog UI

The Unity Catalog UI allows you to interact with a Unity Catalog server to view or create data and AI assets.  

## Start Unity Catalog UI locally

![UC UI](../assets/images/ui/uc-ui-expanded.png)

To use the Unity Catalog UI, start a new terminal and ensure you have already started the UC server (e.g., `./bin/start-uc-server`)

!!! warning "Prerequisites"
    The Unity Catalog UI requires both [Node](https://nodejs.org/en/download/package-manager) and [Yarn](https://classic.yarnpkg.com/lang/en/docs/install).


To start the UI locally, run the following commands to start `yarn`

```
cd /ui
yarn install
yarn start
```

## Assets

### Catalogs

The following steps show how you can create, describe, and delete UC catalogs.

=== "1. List and create catalogs"
    
    After clicking the top **Catalog** button, you will see your list of catalogs.  To create a catalog, click the **Create Catalog** button to the right.

    ![](../assets/images/ui/uc-ui-catalog-create-1.png)
    

=== "2. Create catalog dialog"
    
    Specify the name and include any comments when creating your catalog.

    ![](../assets/images/ui/uc-ui-catalog-create-2.png)

=== "3. Delete a catalog"

    Click the *horizontal three dots* next to **Create Schema** button to delete your catalog.

    ![](../assets/images/ui/uc-ui-catalog-delete.png)

<hr style="height:2px;border-width:0;color:gray;background-color:lightgrey">


### Schemas

The following steps show how you can create, describe, and delete UC schemas.

=== "1. List and create schemas"
    
    After clicking on any catalog, the main dialog contains the list of available schemas.  Click the **Create Schemas** button to the right to create a new schema.

    ![](../assets/images/ui/uc-ui-schema-create-1.png)
    

=== "2. Create schemas dialog"
    
    Specify the name and include any comments when creating your schema.

    ![](../assets/images/ui/uc-ui-schema-create-2.png)

=== "3. Delete a schema"

    Click the *horizontal three dots* to the rightto delete your schema.

    ![](../assets/images/ui/uc-ui-schema-delete.png)

<hr style="height:2px;border-width:0;color:gray;background-color:lightgrey">




### Tables

The following steps show how you can view your UC table metadata and descriptions.

=== "1. View tables in schema (1)"
    
    Click on the schema (e.g., `unity.demo`) to view its tables.

    ![](../assets/images/ui/uc-ui-tables-1.png)
    
=== "2. View tables in schema (2)"
    
    Click on the schema (e.g., `unity.default`) to view its tables.

    ![](../assets/images/ui/uc-ui-tables-2.png)

=== "3. View table metadata"

    Click on any table (e.g., `unity.default.marksheet`) to view its metadata.  You also have the option to delete the table via the *three horizontal dots* on the right.

    ![](../assets/images/ui/uc-ui-tables-3.png)

<hr style="height:2px;border-width:0;color:gray;background-color:lightgrey">


### Volumes

The following steps show how you can view your UC volume metadata and descriptions.

=== "1. Traverse to volumes"
    
    Using the left-hand nav bar, click on *catalog > schema* (e.g., `unity` > `default`) to view the available volumes.

    ![](../assets/images/ui/uc-ui-volumes-1.png)
    
=== "2. View volume metadata"
    
    Click on the volume (e.g., `unity.default.txt_files`) to view its metadata.  You have the option to delete it by click on the *three horizontal dots* to the right. 

    ![](../assets/images/ui/uc-ui-volumes-2.png)

=== "3. Edit volume description"

    Click the edit button to change its descripton.

    ![](../assets/images/ui/uc-ui-volumes-3.png)

<hr style="height:2px;border-width:0;color:gray;background-color:lightgrey">


### Functions

The following steps show how you can view your UC functions metadata and descriptions.

=== "1. Traverse to functions"
    
    Using the left-hand nav bar, click on *catalog > schema* (e.g., `unity` > `default`) to view the available functions.

    ![](../assets/images/ui/uc-ui-functions-1.png)
    
=== "2. View functions metadata"
    
    Click on the volume (e.g., `unity.default.lowercase`) to view its metadata.  You have the option to delete it by click on the *three horizontal dots* to the right. 

    ![](../assets/images/ui/uc-ui-functions-2.png)

<hr style="height:2px;border-width:0;color:gray;background-color:lightgrey">


### Models

The following steps show how you can view your UC models and its metadata.

=== "1. Traverse to models"
    
    Using the left-hand nav bar, click on *catalog > schema* (e.g., `unity` > `default`) to view the available models.

    ![](../assets/images/ui/uc-ui-models-1.png)
    
=== "2. View model versions"
    
    Click on the model (e.g., `unity.default.iris`) to view its version(s).

    ![](../assets/images/ui/uc-ui-models-2.png)

=== "3. View model version metadata"

    Click on the model version to view its details.

    ![](../assets/images/ui/uc-ui-models-3.png)

<hr style="height:2px;border-width:0;color:gray;background-color:lightgrey">