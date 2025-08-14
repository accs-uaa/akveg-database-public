-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query projects
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated:  2022-10-18
-- Usage: Script should be executed in a PostgreSQL 14+ database.
-- Description: "Query projects" queries the project metadata.
-- ---------------------------------------------------------------------------

-- Compile project data
SELECT project.project_code as project_code
     , project.project_name as project_name
     , originator.organization as originator
     , funder.organization as funder
     , personnel.personnel as manager
     , completion.completion as project_status
     , project.year_start as year_start
     , project.year_end as year_end
     , project.project_description
     , project.private as private
FROM project
    LEFT JOIN organization originator ON project.originator_id = originator.organization_id
    LEFT JOIN organization funder ON project.funder_id = funder.organization_id
    LEFT JOIN personnel ON project.manager_id = personnel.personnel_id
    LEFT JOIN completion ON project.completion_id = completion.completion_id
ORDER BY year_start DESC, project_code ASC;
