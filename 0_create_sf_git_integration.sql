/***************************************************************************************************          
Trading data analytichands on lab
Creates database, schemas, stages, file formats, and tables for the data pipeline
Run this script once to set up your Snowflake environment
****************************************************************************************************/

-- Create an API integration so Snowflake can access a github repository
CREATE OR REPLACE API INTEGRATION GITHUB_INTEGRATION
  API_PROVIDER = git_https_api
  API_ALLOWED_HOSTS = ('https://github.com')
  ENABLED = TRUE;

-- Check if integration
SHOW API INTEGRATIONS;
