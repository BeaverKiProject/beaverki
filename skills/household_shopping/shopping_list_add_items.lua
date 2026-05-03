local function trim(text)
    if text == nil then
        return ""
    end
    return (text:gsub("^%s+", ""):gsub("%s+$", ""))
end

local function lower(text)
    return string.lower(trim(text))
end

local function search_page(ctx)
    local query = trim(ctx.input.page_query)
    if query == "" then
        query = "shopping list"
    end
    local title_hint = lower(ctx.input.page_title_hint)
    local search = ctx.tool_call("notion_search", {
        query = query,
        object_kind = "page",
        page_size = ctx.input.search_page_size
    })

    local best = nil
    local best_score = -1
    for _, result in ipairs(search.results or {}) do
        local title_lower = lower(result.title)
        local score = 0

        if title_hint ~= "" then
            if title_lower == title_hint then
                score = 300
            elseif string.find(title_lower, title_hint, 1, true) ~= nil then
                score = 200
            end
        end

        if query ~= "" then
            local query_lower = lower(query)
            if title_lower == query_lower then
                score = math.max(score, 250)
            elseif string.find(title_lower, query_lower, 1, true) ~= nil then
                score = math.max(score, 150)
            end
        end

        if best == nil or score > best_score then
            best = result
            best_score = score
        end
    end

    if best == nil or trim(best.id) == "" then
        error("shopping_list_add_items could not resolve a shopping list page from Notion search")
    end

    return {
        ref = best.id,
        source = "search",
        search_query = query,
    }
end

local function resolve_page(ctx)
    if trim(ctx.input.page_ref) ~= "" then
        return {
            ref = ctx.input.page_ref,
            source = "page_ref",
            search_query = nil,
        }
    end

    return search_page(ctx)
end

return function(ctx)
    local lines = {}
    local normalized_items = {}

    for _, item in ipairs(ctx.input.items or {}) do
        local text = trim(item)
        if text ~= "" then
            table.insert(lines, "- [ ] " .. text)
            table.insert(normalized_items, text)
        end
    end

    if #normalized_items == 0 then
        error("shopping_list_add_items requires at least one non-empty item")
    end

    local resolved_page = resolve_page(ctx)
    local ok, result = pcall(ctx.tool_call, "notion_append_block_children", {
        parent_ref = resolved_page.ref,
        content = table.concat(lines, "\n"),
        position = "end",
        after_block_ref = nil,
    })
    if not ok and resolved_page.source == "page_ref" then
        local original_error = result
        resolved_page = search_page(ctx)
        ok, result = pcall(ctx.tool_call, "notion_append_block_children", {
            parent_ref = resolved_page.ref,
            content = table.concat(lines, "\n"),
            position = "end",
            after_block_ref = nil,
        })
        if not ok then
            error("shopping_list_add_items could not append to explicit page_ref and fallback search also failed: " .. tostring(original_error) .. " / " .. tostring(result))
        end
        resolved_page.source = "page_ref_fallback_search"
    elseif not ok then
        error(result)
    end

    return {
        page_ref = resolved_page.ref,
        resolved_via = resolved_page.source,
        search_query = resolved_page.search_query,
        added_count = #normalized_items,
        items = normalized_items,
        appended_block_ids = result.appended_block_ids,
    }
end
