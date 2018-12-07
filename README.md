# glacier-archive
Scripts for archiving data to AWS Glacier.

A script for uploading data to AWS Glacier vaults.

### Features

* Incremental backups.
* Parallel multipart uploading for large files.
* Generates local index file for easy retrieval.

## Usage

`BackupDirectory <src> [src...] <tar>`

Backup the contents of the directory at `src` (and optionally additional directories) and create an archive at `tar`. The tar is also uploaded to the AWS Glacier vault specified in the configuration.

## Configuration

The script uses AWS named profile configuration configured in the user's home directory. See https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html for more information.

Create a configuration file named `glacier.properties` and place on the classpath.

| Property name      | Description
| ----               | ----                
| aws.profile        | Name of the AWS profile to use (see above)
| glacier.vault.name | Name of the AWS S3 Glacier vault to upload to

