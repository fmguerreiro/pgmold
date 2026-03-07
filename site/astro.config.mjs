// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
	site: 'https://pgmold.dev',
	integrations: [
		starlight({
			title: 'pgmold',
			logo: {
				src: './public/logo.png',
			},
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/fmguerreiro/pgmold' },
			],
			customCss: ['./src/styles/custom.css'],
			sidebar: [
				{ label: 'Home', link: '/' },
				{
					label: 'Getting Started',
					items: [
						{ label: 'Installation', slug: 'getting-started/installation' },
						{ label: 'Quick Start', slug: 'getting-started/quick-start' },
					],
				},
				{
					label: 'Guides',
					items: [
						{ label: 'Multi-File Schemas', slug: 'guides/multi-file-schemas' },
						{ label: 'Filtering Objects', slug: 'guides/filtering' },
						{ label: 'Adopting pgmold', slug: 'guides/adopting' },
						{ label: 'CI/CD Integration', slug: 'guides/ci-cd' },
						{ label: 'Drizzle ORM', slug: 'guides/drizzle' },
						{ label: 'Safety Rules', slug: 'guides/safety' },
					],
				},
				{
					label: 'Reference',
					items: [
						{ label: 'CLI Commands', slug: 'reference/cli' },
						{ label: 'Terraform Provider', slug: 'reference/terraform' },
						{ label: 'GitHub Action', slug: 'reference/github-action' },
					],
				},
				{
					label: 'Comparisons',
					items: [
						{ label: 'pgmold vs Others', slug: 'comparisons/overview' },
					],
				},
			],
		}),
	],
});
