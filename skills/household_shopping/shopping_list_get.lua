local function trim(text)
    if text == nil then
        return ""
    end
    return (text:gsub("^%s+", ""):gsub("%s+$", ""))
end

local function lower(text)
    return string.lower(trim(text))
end

local function resolve_page(ctx)
    if trim(ctx.input.page_ref) ~= "" then
        return {
            ref = ctx.input.page_ref,
            title = nil,
            source = "page_ref",
            search_query = nil,
        }
    end

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
        local title = trim(result.title)
        local title_lower = lower(title)
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

        if score == 0 then
            score = 100
        end

        if best == nil or score > best_score then
            best = result
            best_score = score
        end
    end

    if best == nil or trim(best.id) == "" then
        error("shopping_list_get could not resolve a shopping list page from Notion search")
    end

    return {
        ref = best.id,
        title = best.title,
        source = "search",
        search_query = query,
    }
end

local function normalize_items(blocks, include_checked)
    local items = {}
    for _, block in ipairs(blocks or {}) do
        local block_type = block.type
        local text = trim(block.text)
        local checked = block.checked == true
        local is_item = block_type == "to_do"
            or block_type == "bulleted_list_item"
            or block_type == "numbered_list_item"

        if is_item and text ~= "" and (include_checked or not checked) then
            table.insert(items, {
                block_id = block.id,
                text = text,
                checked = checked,
                kind = block_type,
            })
        end
    end
    return items
end

return function(ctx)
    local include_checked = ctx.input.include_checked == true
    local resolved_page = resolve_page(ctx)
    local page = ctx.tool_call("notion_fetch", {
        ref = resolved_page.ref,
        object_kind = "page",
        include_content = true,
        page_size = ctx.input.page_size
    })

    local blocks = {}
    if page.content ~= nil and page.content.blocks ~= nil then
        blocks = page.content.blocks
    end

    local items = normalize_items(blocks, include_checked)
    return {
        page_ref = resolved_page.ref,
        page_id = page.id,
        page_title = page.title,
        resolved_via = resolved_page.source,
        search_query = resolved_page.search_query,
        item_count = #items,
        items = items,
    }
end
