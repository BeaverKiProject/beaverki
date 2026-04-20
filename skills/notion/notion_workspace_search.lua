return function(ctx)
    return ctx.tool_call("notion_search", {
        query = ctx.input.query,
        object_kind = ctx.input.object_kind,
        page_size = ctx.input.page_size
    })
end
