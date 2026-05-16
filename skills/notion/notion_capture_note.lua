return function(ctx)
    return ctx.tool_call("notion_create_page", {
        parent_kind = ctx.input.parent_kind,
        parent_ref = ctx.input.parent_ref,
        title = ctx.input.title,
        content = ctx.input.content,
        properties_json = ctx.input.properties_json,
        template_type = ctx.input.template_type,
        template_ref = ctx.input.template_ref,
        template_timezone = ctx.input.template_timezone
    })
end
