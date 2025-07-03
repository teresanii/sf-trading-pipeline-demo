USE ROLE ACCOUNTADMIN;

-- Create an API integration so Snowflake can access a github repository
CREATE OR REPLACE API INTEGRATION GITHUB_INTEGRATION
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com')
  ENABLED = TRUE;

-- Check if integration
SHOW API INTEGRATIONS;
