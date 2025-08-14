-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query projects
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated:  2025-04-01
-- Usage: Script should be executed in a PostgreSQL 14+ database.
-- Description: "Query projects" queries the project metadata.
-- ---------------------------------------------------------------------------

-- Compile project data
SELECT project.project_code as prjct_cd
     , project.project_name as prjct_nm
     , originator.organization as originator
     , funder.organization as funder
     , personnel.personnel as manager
     , completion.completion as prjct_status
     , project.year_start as year_start
     , project.year_end as year_end
     , project.project_description as prjct_desc
     , project.private as private
FROM project
    LEFT JOIN organization originator ON project.originator_id = originator.organization_id
    LEFT JOIN organization funder ON project.funder_id = funder.organization_id
    LEFT JOIN personnel ON project.manager_id = personnel.personnel_id
    LEFT JOIN completion ON project.completion_id = completion.completion_id
ORDER BY year_start DESC, project_code ASC;
