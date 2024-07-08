# Unity Catalog Volumes

Let's list the volumes.

```sh
bin/uc volume list --catalog unity --schema default
```

You should see a few volumes. Let's get the metadata of one of those volumes.

```sh
bin/uc volume get --full_name unity.default.json_files
```

Now let's list the directories/files in this volume.

```sh
bin/uc volume read --full_name unity.default.json_files
```

You should see two text files listed and one directory. Let's read the content of one of those files.

```sh
bin/uc volume read --full_name unity.default.json_files --path c.json
```

Voila! You have read the content of a file stored in a volume. We can also list the contents of any subdirectory. For example:

```sh
bin/uc volume read --full_name unity.default.json_files --path dir1
```

Now let's try creating a new external volume. First physically create a directory with some files in it.
For example, create a directory `/tmp/myVolume` and put some files in it.
Then create the volume in UC.

```sh
bin/uc volume create --full_name unity.default.myVolume --storage_location /tmp/myVolume
```

Now you can see the contents of this volume.

```sh
bin/uc volume read --full_name unity.default.myVolume
```
