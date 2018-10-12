# supporting views


caom2.ObsCore.sql   : ObsCore-1.1 view of caom2 tables
caom2.ObsCore-x.sql : ObsFile and ObsPart views of caom2 is a custom extension proposed to IVOA

caom2.SIAv1.sql : view to provide correct output for SIA-1.0 service (SIA-2.0 service uses the ObsCore view)

ivoa.tap_schema_content11.sql  : tap_schema (version 1.1) description of all ivoa tables and views
caom2.tap_schema_content11.sql : tap_schema (version 1.1) description of all caom2 tables and views

# pre-computed aggregate views

These views (tables) support data exploration (for example: by the CADC search portal). Use the views
if you have a small collection and want new entries to be immediately visible in these views. Use the
materialised views for a large collection (faster queries than simple view) where content is more stable.
The materialised view SQL files are intended to be run periodically (once per day?) as they create a new
table and swap it for the previous one in a transaction so the update does not effect existing usage. Experience
at CADC is that some of these take 10s of minutes to run when there are ~10 million observations and/or planes
in the database... at that scale once per day seems appropriate.

These fields are used for tab-completion and rapid ui-tap searching:

caom2.distinct_proposal_id.sql (materialised)  
caom2.distinct_proposal_id_view.sql (view)

caom2.distinct_proposal_pi.sql (materialised)
caom2.distinct_proposal_pi_view.sql (view)

caom2.distinct_proposal_title.sql (materialised)
caom2.distinct_proposal_title_view.sql (view)

These fields are used to generate picklists, especially linked picklists, as they provide a "group by" view
of all combinations of enumerated fields:

caom2.EnumField.sql (materialised)
caom2.EnumField_view.sql (view)

caom2.ObsCoreEnumField.sql (materialised)
caom2.ObsCoreEnumField_view.sql (view)

