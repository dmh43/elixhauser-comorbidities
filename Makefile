## --------------------------------------------------------------------------
## Description: Automated creation of the eICU database and importing of data
## --------------------------------------------------------------------------


## Parameters ##

DBNAME=eicu
DBUSER=eicu
DBPASS=EICU
DBHOST=localhost
DBSCHEMA=public

## Commands ##
PSQL=psql "dbname=$(DBNAME) options=--search_path=$(DBSCHEMA)" --username=$(DBUSER)

## Export ##
# Parameters given in this Makefile take precedence over those defined in each
# individual Makefile (due to specifying the -e option and the export command
# here)
export


icd9:
	@echo ''
	@echo '-------------------'
	@echo '-- Generating ICD9 View --'
	@echo '-------------------'
	@echo ''
	@sleep 2
	PGPASSWORD=$(DBPASS) psql -v lim=10000 "dbname=${DBNAME} options=--search_path=${DBSCHEMA}" -f elixhauser-ahrq-v37-no-drg-all-icd.sql --username="$(DBUSER)" --host="$(DBHOST)"

icd9-limit:
	@echo ''
	@echo '-------------------'
	@echo '-- Generating ICD9 View --'
	@echo '-------------------'
	@echo ''
	@sleep 2
	PGPASSWORD=$(DBPASS) psql -v lim=10000 "dbname=${DBNAME} options=--search_path=${DBSCHEMA}" -f elixhauser-ahrq-v37-no-drg-all-icd.sql --username="$(DBUSER)" --host="$(DBHOST)"

.PHONY: help eicu
