# Overview

The current version of the documentation for this system and its data outputs is in '/home/joel/core/docs/housing-docs/housing-layouts-and-rules.md', and is published online as a Quarto based static site hosted on an Azure storage blob account that has been enabled for website serving. The site itself also links to a pdf based version that is an integral part of the publishing process. Publishing of this site is under the review of BMAD agent Freddy the publishing director, and site history for this site is stored in '/home/joel/core/.bmad-user-memory/publishing-director/memories.md'. Please also review Freddy's capabilities, and please invoke his assistance if it will be useful.

## GUIDELINES

Here are some guidelines for the new documentation; some of the implementation detail and sections included in the current documentation are not wanted in the new document; see below.

## GENERAL:

This document is intended for an *audience of model builders and analysts who do not have an interest or need to know about the internal build mechanics (directories, technology platform and deployment architecture, etc);* these documents are also needed, but will be added *later*, for an operatinal/internal audience. This documentation is intended to support a paid product.

GOLDEN RULE: no detail is extracted and included in this document unless it comes either from 1: code in '01_housing' as implemented, or 2: the '/home/joel/data/housing_distro.duckdb' data structure and contents, which is absolute ground truth, and is what is being delivered to clients. No technical detail should be assumed, or inferred from indirect or linked sources on the internet, Census Bureau pages, etc

## NOT wanted:

- Implementation/platform details
- R version
- Directories
- Detailed code linkage explainers
- Beginner explainers

## Wanted:

- Table layouts and dictionaries
- Source data documentation and vintage
- Calculation rules 
- Supporting dictionaries and inventory lists; ex: list of CBSAs, Urban Areas, Counties, etc. in appendices

## Things to enable as part of the implementation:

- The Quarto implementation generates a limited number of 'page break' markers designed for Typst to break pages at headers; these were not fully implemented for the existing site/document, however a general desire is to have the PDF version of this new version more fully implement formatting so page breaks happen at headers if possible.

## METADATA

In the production database listed above, there is a 'metadata' table that lists the tables and column definitions in the associated 11 database tables; please makek use of this as needed.

## CLIENT PROVISIONING OVERVIEW

Each client served by this product will have a provisioned data enclave on MS Azure, based on the process described here, which is the main client enablement application:

/home/joel/00_clientinfra

After the associated Github action runs, there is a private data portal using a basic Shiny app that serves as the distribution channel to and from the client. This document should contain a brief overview of this infrastructure, and tie it in with the actual data deliverables desscribed in the document. Make sure to include a best practices flow description  for initial distribution of the SAS URL on client provisioning; the data deliverables here will be waiting for client download at this URL, which is a basic, client-specific Shiny app and data infrastructure (keyvault, storage account, etc)
