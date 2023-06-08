
--[[
Parquet schema change check

Args:
 - locations (list of strings): locations to look for parquet files under
 - sample (boolean): whether reading one new/changed file per directory is enough, or go through all of them
]]

lakefs = require("lakefs")
strings = require("strings")
parquet = require("encoding/parquet")
regexp = require("regexp")
path = require("path")

visited_directories = {}
schema_changed = false
schema_changes = ""
delimiter = ""

for _, location in ipairs(args.locations) do
    after = ""
    has_more = true
    need_more = true
    print("checking location: " .. location)
    while has_more do
        print("running diff, location = " .. location .. " after = " .. after)
        -- Diff between destination and source ref for a particular location
        local code, resp = lakefs.diff_refs(action.repository_id, action.branch_id, action.source_ref, after, location)
        if code ~= 200 then
            error("could not diff: " .. resp.message)
        end

        for _, result in pairs(resp.results) do
            p = path.parse(result.path)
            print("checking: '" .. result.path .. "'")
            if not args.sample or (p.parent and not visited_directories[p.parent]) then
                if result.path_type == "object" and result.type ~= "removed" then
                    if strings.has_suffix(p.base_name, ".parquet") then
                        -- Get the original schema if Parquet files exist in the destination ref
                        destination_list_objects = nil
                        destination_schema = nil
                        -- Get the list of objects in the parent directory from the destination ref
                        code, destination_list_objects = lakefs.list_objects(action.repository_id, action.branch_id, after, p.parent, delimiter)
                        if code ~= 200 then
                            error("could not list objects: HTTP " .. tostring(code) .. "body:\n" .. destination_list_objects)
                        end
                        
                        for _, list_objects_result in pairs(destination_list_objects.results) do
                            list_objects_path = path.parse(list_objects_result.path)
                            -- If there is a Parquet file in the parent directory in the destination ref
                            if strings.has_suffix(list_objects_path.base_name, ".parquet") then
                                code, destination_content = lakefs.get_object(action.repository_id, action.branch_id, list_objects_result.path)
                                if code ~= 200 then
                                    error("could not fetch data file: HTTP " .. tostring(code) .. "body:\n" .. destination_content)
                                end
                                destination_schema = parquet.get_schema(destination_content)
                                break
                            end
                        end
                        
                        -- If list_objects is not empty and destination_schema is available then check if schema changed
                        if next(destination_list_objects.results) ~= nil and destination_schema ~= nil then
                            -- Get Parquet file from source ref
                            code, source_content = lakefs.get_object(action.repository_id, action.source_ref, result.path)
                            if code ~= 200 then
                                error("could not fetch data file: HTTP " .. tostring(code) .. "body:\n" .. source_content)
                            end
                            -- Extract schema from source Parquet file
                            source_schema = parquet.get_schema(source_content)
                                 
                            -- Check each column name and data type between source and destination ref
                            for i, source_column in ipairs(source_schema) do
                                if destination_schema[i].name ~= source_column.name then
                                    schema_changed = true
                                    schema_changes = schema_changes .. "Schema changed for '" ..  result.path .. "'. "
                                    schema_changes = schema_changes .. "Column name changed. Original column name was '" .. destination_schema[i].name .. "' and new column name is '" .. source_column.name .. "'. "
                                    print("\t Column name changed. Original column name was '" .. destination_schema[i].name .. "' and new column name is '" .. source_column.name .. "'")
                                end
                                if destination_schema[i].type ~= source_column.type then
                                    schema_changed = true
                                    schema_changes = schema_changes .. "Schema changed for '" ..  result.path .. "'. "
                                    schema_changes = schema_changes .. "Data type for column '" .. destination_schema[i].name .. "' changed. Original data type was '" .. destination_schema[i].type .. "' and new data type is '" .. source_column.type .. "'. "
                                    print("\t Data type for column '" .. destination_schema[i].name .. "' changed. Original data type was '" .. destination_schema[i].type .. "' and new data type is '" .. source_column.type .. "'")
                                end
                            end
                            
                            if schema_changed then
                                print("\t Schema changed. Review schema changes above.")
                            else
                                print("\t all columns are valid")
                            end
                            visited_directories[p.parent] = true
                        end
                    end
                end
            else
                print("\t skipping path, directory already sampled")
            end
        end

        -- pagination
        has_more = resp.pagination.has_more
        after = resp.pagination.next_offset
    end
end

if schema_changed then
    error(schema_changes)
end
