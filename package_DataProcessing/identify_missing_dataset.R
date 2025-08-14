# Script to verify which project is missing from the list

project_list = data.frame(project = NA)

for (i in 1:length(files_list)) {
  file_path = files_list[[i]]
  project_name = str_split_i(file_path, pattern = "/", i = 9)
  project_list[i,1] = project_name
}

project_list = as.character(project_list$project)

target_paths[which(!(target_paths %in% project_list))]
