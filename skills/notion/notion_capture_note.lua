return function(ctx)
    return ctx.tool_call("notion_create_page", {
        parent_kind = ctx.input.parent_kind,
        parent_ref = ctx.input.parent_ref,
        title = ctx.input.title,
        content = ctx.input.content
    })
end
