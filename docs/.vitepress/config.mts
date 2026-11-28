import { execSync } from 'node:child_process';
import path from 'node:path';
import { withMermaid } from 'vitepress-plugin-mermaid';

function resolveVersion(): string {
  const envVersion = process.env.SEMAPHORE_VERSION?.trim();
  if (envVersion) {
    return envVersion;
  }
  try {
    const tag = execSync("git describe --tags --abbrev=0 --match 'v[0-9]*'", {
      stdio: ['ignore', 'pipe', 'ignore'],
    })
      .toString()
      .trim();
    if (tag) {
      return tag;
    }
  } catch {
    // No release tag present in repo yet
  }
  return 'dev';
}

function rewriteHtmlMdLinks(content: string): string {
  return content.replace(/href="([^"#]+)\.md(#[^"]*)?"/g, (match, base: string, anchor = '') => {
    if (base.startsWith('http://') || base.startsWith('https://')) {
      return match;
    }
    if (base === 'README' || base.endsWith('/README')) {
      const cleanBase = base.slice(0, -'README'.length) || './';
      return `href="${cleanBase}${anchor}"`;
    }
    return `href="${base}${anchor}"`;
  });
}

const version = resolveVersion();
const rawBase = process.env.DOCS_BASE?.trim() || '/';
const base = rawBase.endsWith('/') ? rawBase : `${rawBase}/`;

export default withMermaid({
  base,
  title: 'Semaphore',
  description: 'Self-hosted feature flag relay proxy and Go engine with OpenFeature OFREP support',
  cleanUrls: true,
  lastUpdated: true,
  srcExclude: ['**/node_modules/**'],
  ignoreDeadLinks: [/^https?:\/\/localhost/],

  rewrites: {
    'README.md': 'overview.md',
    ':dir(.*)/README.md': ':dir/index.md',
  },

  markdown: {
    config(md) {
      const defaultCodeInline = md.renderer.rules.code_inline;
      md.renderer.rules.code_inline = (tokens, idx, options, env, self) => {
        const token = tokens[idx];
        if (token.content.includes('{{') || token.content.includes('}}')) {
          token.attrSet('v-pre', '');
        }
        return defaultCodeInline
          ? defaultCodeInline(tokens, idx, options, env, self)
          : self.renderToken(tokens, idx, options);
      };

      for (const ruleName of ['html_block', 'html_inline'] as const) {
        const defaultHtmlRule = md.renderer.rules[ruleName];
        md.renderer.rules[ruleName] = (tokens, idx, options, env, self) => {
          tokens[idx].content = rewriteHtmlMdLinks(tokens[idx].content);
          return defaultHtmlRule
            ? defaultHtmlRule(tokens, idx, options, env, self)
            : tokens[idx].content;
        };
      }

      const defaultLinkOpen = md.renderer.rules.link_open;
      md.renderer.rules.link_open = (tokens, idx, options, env, self) => {
        const token = tokens[idx];
        const hrefIndex = token.attrIndex('href');
        if (hrefIndex >= 0 && env?.relativePath) {
          const href = token.attrs![hrefIndex][1];
          if (href && !href.startsWith('http://') && !href.startsWith('https://') && !href.startsWith('#') && !href.startsWith('/')) {
            const [rawPath, hash] = href.split('#');
            const dir = path.posix.dirname(env.relativePath);
            const resolved = path.posix.normalize(path.posix.join(dir === '.' ? '' : dir, rawPath));
            if (resolved.startsWith('../')) {
              const repoRel = resolved.replace(/^(\.\.\/)+/, '');
              const anchor = hash ? `#${hash}` : '';
              token.attrSet('href', `https://github.com/BlackbirdWorks/semaphore/blob/main/${repoRel}${anchor}`);
              token.attrSet('target', '_blank');
              token.attrSet('rel', 'noreferrer');
            }
          }
        }
        const rendered = defaultLinkOpen
          ? defaultLinkOpen(tokens, idx, options, env, self)
          : self.renderToken(tokens, idx, options);

        // VitePress resolves relative README.md links against the source filename, emitting
        // .../README instead of .../ (from our README.md -> index.md rewrite). Normalize the
        // emitted relative href so every in-site README link hits the published directory index.
        return rendered.replace(
          /href="((?!https?:\/\/)[^"#]*\/)?README(?:\.md)?(#[^"]*)?"/g,
          (_m, prefix = '', anchor = '') => `href="${prefix || './'}${anchor}"`,
        );
      };
    },
  },

  head: [
    ['link', { rel: 'icon', type: 'image/svg+xml', href: `${base}favicon.svg?v=2` }],
  ],

  themeConfig: {
    logo: '/logo.svg?v=2',

    search: {
      provider: 'local',
    },

    outline: {
      level: [2, 3],
      label: 'On this page',
    },

    docFooter: {
      prev: 'Previous',
      next: 'Next',
    },

    nav: [
      { text: 'Docs', link: '/relay-proxy/getting-started', activeMatch: '^/(relay-proxy|concepts|configure-flags)/' },
      {
        text: 'Integrations',
        activeMatch: '^/integrations/',
        items: [
          { text: 'Store Flags (Retrievers)', link: '/integrations/retrievers/' },
          { text: 'Export Data (Exporters)', link: '/integrations/exporters/' },
          { text: 'Notify Changes (Notifiers)', link: '/integrations/notifiers/' },
        ],
      },
      { text: 'SDKs & OFREP', link: '/openfeature/ofrep', activeMatch: '^/(openfeature|tracking)/' },
      { text: 'Go Module', link: '/go-module/getting-started', activeMatch: '^/go-module/' },
      { text: 'Tooling', link: '/tooling/cli', activeMatch: '^/tooling/' },
      {
        text: version,
        items: [
          { text: 'Documentation Index', link: '/overview' },
          { text: 'Performance', link: '/performance' },
          { text: 'Scale', link: '/scale' },
          { text: 'Releasing', link: '/RELEASING' },
          { text: 'GitHub Releases', link: 'https://github.com/BlackbirdWorks/semaphore/releases' },
        ],
      },
    ],

    sidebar: [
      {
        text: 'Getting Started',
        collapsed: false,
        items: [
          { text: 'Documentation Index', link: '/overview' },
          { text: 'Quick Start', link: '/relay-proxy/getting-started' },
          { text: 'Quick Start (file store)', link: '/relay-proxy/getting-started-file-store' },
          { text: 'Installation', link: '/relay-proxy/install' },
          { text: 'Architecture', link: '/concepts/architecture' },
        ],
      },
      {
        text: 'Relay Proxy',
        collapsed: false,
        items: [
          { text: 'Overview', link: '/relay-proxy/' },
          { text: 'Bootstrap File', link: '/relay-proxy/bootstrap-config' },
          { text: 'Schema Reference', link: '/reference/schema' },
          { text: 'Relay Core', link: '/relay-proxy/relay-core' },
          { text: 'Probes', link: '/relay-proxy/probes' },
          { text: 'Runbook', link: '/relay-proxy/runbook' },
          { text: 'Security', link: '/relay-proxy/security' },
          { text: 'API Endpoints', link: '/relay-proxy/endpoints' },
          { text: 'Client Compatibility', link: '/relay-proxy/client-compatibility' },
          { text: 'Observability', link: '/relay-proxy/observability' },
          { text: 'Profiling', link: '/relay-proxy/profiling' },
          { text: 'Advanced Usage', link: '/relay-proxy/advanced-usage' },
        ],
      },
      {
        text: 'Configure Flags',
        collapsed: false,
        items: [
          { text: 'Flag Format', link: '/configure-flags/create-flags' },
          { text: 'Targeting Rules', link: '/configure-flags/target-with-flags' },
          { text: 'Segments', link: '/configure-flags/segments' },
          { text: 'Segments - static', link: '/configure-flags/segments-static' },
          { text: 'Inline lists or dynamic segments', link: '/configure-flags/segments-choosing' },
          {
            text: 'Segments - dynamic',
            link: '/configure-flags/segments-dynamic',
            collapsed: true,
            items: [
              { text: 'Redis', link: '/configure-flags/segments-redis' },
              { text: 'Valkey', link: '/configure-flags/segments-valkey' },
            ],
          },
          { text: 'Custom Bucketing', link: '/configure-flags/custom-bucketing' },
          {
            text: 'Rollout Strategies',
            collapsed: false,
            items: [
              { text: 'Overview', link: '/configure-flags/rollout-strategies/' },
              { text: 'Percentage', link: '/configure-flags/rollout-strategies/percentage' },
              { text: 'Progressive', link: '/configure-flags/rollout-strategies/progressive' },
              { text: 'Scheduled', link: '/configure-flags/rollout-strategies/scheduled' },
              { text: 'Experimentation', link: '/configure-flags/rollout-strategies/experimentation' },
            ],
          },
        ],
      },
      {
        text: 'Store Flags (Retrievers)',
        collapsed: true,
        items: [
          { text: 'All Retrievers', link: '/integrations/retrievers/' },
          { text: 'File System', link: '/integrations/retrievers/file' },
          { text: 'HTTP(S)', link: '/integrations/retrievers/http' },
          { text: 'AWS S3', link: '/integrations/retrievers/aws-s3' },
          { text: 'MinIO', link: '/integrations/retrievers/minio' },
          { text: 'Google Cloud Storage', link: '/integrations/retrievers/google-cloud-storage' },
          { text: 'Azure Blob Storage', link: '/integrations/retrievers/azure-blob-storage' },
          { text: 'GitHub', link: '/integrations/retrievers/github' },
          { text: 'GitLab', link: '/integrations/retrievers/gitlab' },
          { text: 'Bitbucket', link: '/integrations/retrievers/bitbucket' },
          { text: 'Kubernetes ConfigMap', link: '/integrations/retrievers/kubernetes-configmap' },
          { text: 'MongoDB', link: '/integrations/retrievers/mongodb' },
          { text: 'PostgreSQL', link: '/integrations/retrievers/postgresql' },
          { text: 'Redis', link: '/integrations/retrievers/redis' },
        ],
      },
      {
        text: 'Export Data (Exporters)',
        collapsed: true,
        items: [
          { text: 'All Exporters', link: '/integrations/exporters/' },
          { text: 'File', link: '/integrations/exporters/file' },
          { text: 'Log', link: '/integrations/exporters/log' },
          { text: 'Webhook', link: '/integrations/exporters/webhook' },
          { text: 'OpenTelemetry', link: '/integrations/exporters/opentelemetry' },
          { text: 'AWS S3', link: '/integrations/exporters/aws-s3' },
          { text: 'AWS Kinesis', link: '/integrations/exporters/aws-kinesis' },
          { text: 'AWS SQS', link: '/integrations/exporters/aws-sqs' },
          { text: 'AWS Redshift', link: '/integrations/exporters/aws-redshift' },
          { text: 'MinIO', link: '/integrations/exporters/minio' },
          { text: 'Google Cloud Storage', link: '/integrations/exporters/google-cloud-storage' },
          { text: 'Google Cloud Pub/Sub', link: '/integrations/exporters/google-cloud-pubsub' },
          { text: 'Google Cloud BigQuery', link: '/integrations/exporters/google-cloud-bigquery' },
          { text: 'Azure Blob Storage', link: '/integrations/exporters/azure-blob-storage' },
          { text: 'Kafka', link: '/integrations/exporters/kafka' },
        ],
      },
      {
        text: 'Notify Changes (Notifiers)',
        collapsed: true,
        items: [
          { text: 'All Notifiers', link: '/integrations/notifiers/' },
          { text: 'Slack', link: '/integrations/notifiers/slack' },
          { text: 'Discord', link: '/integrations/notifiers/discord' },
          { text: 'Microsoft Teams', link: '/integrations/notifiers/microsoft-teams' },
          { text: 'Webhook', link: '/integrations/notifiers/webhook' },
        ],
      },
      {
        text: 'SDKs & Tracking',
        collapsed: true,
        items: [
          { text: 'OpenFeature Overview', link: '/openfeature/' },
          { text: 'OpenFeature OFREP', link: '/openfeature/ofrep' },
          { text: 'Flag Usage Tracking', link: '/tracking/flag-usage-tracking' },
          { text: 'Tracking API', link: '/tracking/tracking-api' },
        ],
      },
      {
        text: 'Go Module',
        collapsed: true,
        items: [
          { text: 'Getting Started', link: '/go-module/getting-started' },
          { text: 'Configuration', link: '/go-module/configuration' },
          { text: 'Store Flags', link: '/go-module/store_file' },
          { text: 'Target a User', link: '/go-module/target_user' },
          { text: 'Data Collection', link: '/go-module/data_collection' },
          { text: 'Notifiers', link: '/go-module/notifier' },
          { text: 'Control Plane Documents', link: '/go-module/controlplane' },
          { text: 'Document Versions', link: '/go-module/document-versioning' },
          { text: 'Publish to S3', link: '/go-module/s3publish' },
        ],
      },
      {
        text: 'Concepts',
        collapsed: true,
        items: [
          { text: 'Flag Evaluation', link: '/concepts/flag-evaluation' },
          { text: 'Evaluation Context', link: '/concepts/evaluation-context' },
          { text: 'Flag Sets', link: '/concepts/flagset' },
          { text: 'Live Reload', link: '/concepts/live-reload' },
          { text: 'Reload Semantics', link: '/concepts/reload-semantics' },
          { text: 'Manifest and Documents', link: '/concepts/manifest-and-documents' },
          { text: 'Request Budget', link: '/concepts/request-budget' },
          { text: 'Flag Sets and Evaluation', link: '/concepts/flag-sets-and-evaluation' },
          { text: 'Connections', link: '/concepts/connections' },
          { text: 'Events and Notifiers', link: '/concepts/events-and-notifiers' },
          { text: 'Control-Plane Polling', link: '/concepts/control-plane-polling' },
          { text: 'Control-Plane File Store', link: '/concepts/control-plane-file-store' },
          { text: 'Retrievers', link: '/concepts/retriever' },
          { text: 'Exporters', link: '/concepts/exporter' },
          { text: 'Notifiers', link: '/concepts/notifier' },
        ],
      },
      {
        text: 'Tooling',
        collapsed: true,
        items: [
          { text: 'CLI Overview', link: '/tooling/cli' },
          { text: 'Evaluate Flags', link: '/tooling/evaluate' },
          { text: 'Lint Flags', link: '/tooling/linter' },
          { text: 'Generate Manifest', link: '/tooling/generate' },
          { text: 'Publish', link: '/tooling/publish' },
          { text: 'Config', link: '/tooling/config' },
          { text: 'Segment', link: '/tooling/segment' },
          { text: 'JSON Schema Autocomplete', link: '/tooling/autocomplete' },
        ],
      },
      {
        text: 'Reference & Migration',
        collapsed: true,
        items: [
          { text: 'Performance', link: '/performance' },
          { text: 'Scale', link: '/scale' },
          { text: 'Releasing', link: '/RELEASING' },
          { text: 'From go-feature-flag (config v1)', link: '/migration/from-go-feature-flag' },
          { text: 'Rename to Semaphore', link: '/migration/rename-to-semaphore' },
          { text: 'Prometheus Metric Renames', link: '/migration/metric-renames' },
          { text: 'Removal of ffuser', link: '/migration/remove-ffuser' },
          { text: 'Removal of Singleton Client', link: '/migration/remove-singleton' },
          { text: 'JsonLogic v3.11.0 Changes', link: '/migration/jsonlogic-3-11' },
        ],
      },
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/BlackbirdWorks/semaphore' },
    ],

    editLink: {
      pattern: ({ filePath }) => {
        if (filePath === 'overview.md') {
          return 'https://github.com/BlackbirdWorks/semaphore/edit/main/docs/README.md';
        }
        if (filePath.endsWith('/index.md')) {
          const dir = filePath.slice(0, -'/index.md'.length);
          return `https://github.com/BlackbirdWorks/semaphore/edit/main/docs/${dir}/README.md`;
        }
        return `https://github.com/BlackbirdWorks/semaphore/edit/main/docs/${filePath}`;
      },
      text: 'Edit this page on GitHub',
    },

    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2026 BlackbirdWorks',
    },
  },
});
