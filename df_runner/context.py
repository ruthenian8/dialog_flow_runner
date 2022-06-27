from df_engine.core import Context


def merge(*contexts: Context):
    context = {
        'id': contexts[0].id,
        'labels': dict(),
        'requests': dict(),
        'responses': dict(),
        'misc': dict(),
        'validation': contexts[0].validation,
        'framework_states': dict()
    }

    for ctx in contexts:
        for key, label in ctx.labels.items():
            context['labels'][key] = label
        for key, request in ctx.requests.items():
            context['requests'][key] = request
        for key, response in ctx.responses.items():
            context['responses'][key] = response
        for key, misc in ctx.misc.items():
            context['misc'][key] = misc
        for key, framework in ctx.framework_states.items():
            context['framework_states'][key] = dict()
            for state_key, framework_state in framework.items():
                context['framework_states'][key][state_key] = framework_state

    return Context.cast(context)
