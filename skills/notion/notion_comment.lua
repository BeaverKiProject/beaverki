return function(ctx)
    return ctx.tool_call("notion_create_comment", {
        parent_kind = ctx.input.parent_kind,
        parent_ref = ctx.input.parent_ref,
        content = ctx.input.content
    })
end
