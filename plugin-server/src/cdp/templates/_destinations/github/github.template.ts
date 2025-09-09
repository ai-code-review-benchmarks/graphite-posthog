import { HogFunctionTemplate } from '~/cdp/types'

export const template: HogFunctionTemplate = {
    status: 'stable',
    free: false,
    type: 'destination',
    id: 'template-github',
    name: 'GitHub',
    description: 'Creates an issue in a GitHub repository',
    icon_url: '/static/services/github.png',
    category: ['Error tracking'],
    code_language: 'hog',
    code: `fun create_issue() {
    let owner := inputs.github_installation.account.name
    let repo := inputs.repository
    let title := event.properties.name
    let description := event.properties.description
    let posthog_issue_id := event.distinct_id

    if (not owner) {
        throw Error('Owner is required')
    }

    if (not repo) {
        throw Error('Repository is required')
    }

    if (not title) {
        throw Error('Issue title is required')
    }

    if (not description) {
        throw Error('Issue description is required')
    }

    if (not posthog_issue_id) {
        throw Error('PostHog issue ID is required')
    }

    let posthog_issue_url := f'{project.url}/error_tracking/{posthog_issue_id}'
    let payload := {
        'method': 'POST',
        'headers': {
            'Authorization': f'Bearer {inputs.github_installation.access_token}',
            'Accept': 'application/vnd.github+json',
            'Content-Type': 'application/json',
            'User-Agent': 'PostHog Github App'
        },
        'body': {
            'title': title,
            'body': f'{description}\n\n[View in PostHog]({posthog_issue_url})'
        }
    }

    let res := fetch(f'https://api.github.com/repos/{owner}/{repo}/issues', payload)
    if (res.status < 200 or res.status >= 300) {
        throw Error(f'Failed to create GitHub issue: {res.status}: {res.body}')
    }
}

create_issue();`,
    inputs_schema: [
        {
            key: 'github_installation',
            type: 'integration',
            integration: 'github',
            label: 'GitHub installation',
            secret: false,
            hidden: false,
            required: true,
        },
        {
            key: 'repository',
            type: 'integration_field',
            integration_key: 'github_installation',
            integration_field: 'github_repository',
            label: 'Repository',
            secret: false,
            hidden: false,
            required: true,
        },
    ],
}
