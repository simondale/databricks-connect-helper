# Introduction

This repo contains a notebook that demonstrates how to use your Azure AD credentials to authenticate with Data Lake Gen2 when connected with Databricks Connect.

## Problem
The challenge with Databricks Connect and credential passthrough arises because the authentication using a Personal Access Token (PAT) bypasses the Azure AD OAuth flow that sets up the ADLS2 passthrough token in the Databricks context.

The configuration options for Azure AD Passthrough detailed in the Databricks documentation also limit options to:
* Shared Key
* SAS Token
* Client ID and Client Secret
* Client ID and Client Secret at the cluster level

None of these options preserve the identity of the user running code through Databricks Connect.

## Solution
Reading the documentation for the Hadoop Driver for the ABFS protocol reveals some additional configuration options that can be used with Databricks. 

The settings of most interest are related to the **RefreshToken** as a user can request a refresh token when authenticating with Azure AD. By following an OAuth flow that provides access to a Refresh Token, a user can set this in the Spark configuration and Databricks will then authenticate to the Data Lake using the correct account.

Here are the config options to set:

```
spark = SparkSession.builder.getOrCreate()
spark.conf.set('fs.azure.account.auth.type', 'OAuth')
spark.conf.set('fs.azure.account.oauth.provider.type', 'org.apache.hadoop.fs.azurebfs.oauth2.RefreshTokenBasedTokenProvider')
spark.conf.set('fs.azure.account.oauth2.refresh.token', refresh_token)
spark.conf.set('fs.azure.account.oauth2.refresh.endpoint', 'https://login.microsoftonline.com/Common/oauth2/token')
spark.conf.set('fs.azure.account.oauth2.client.id', client_id)
```

This assumes the `client_id` and `refresh_token` variables are already initialised.

To make this work, you will require an application that can provide the following scopes for the `https://storage.azure.com` resource:
* https://storage.azure.com/.default
* offline_access

See the attached notebook for an example.



