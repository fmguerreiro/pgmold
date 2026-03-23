// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

const darkCodeTheme = {
	name: 'pgmold-dark',
	type: 'dark',
	settings: [],
	colors: {
		'editor.background': '#0d1117',
		'editor.foreground': '#c9d1d9',
		'editorGroupHeader.tabsBackground': '#161b22',
		'editorGroupHeader.tabsBorder': '#21262d',
		'tab.activeBackground': '#0d1117',
		'tab.activeBorderTop': '#336df5',
		'tab.inactiveBackground': '#161b22',
		'titleBar.activeBackground': '#161b22',
		'titleBar.border': '#21262d',
		'activityBar.background': '#161b22',
		'sideBar.background': '#161b22',
	},
};

export default defineConfig({
	site: 'https://pgmold.dev',
	base: '/docs',
	outDir: './dist/docs',
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
			expressiveCode: {
				themes: [darkCodeTheme],
				styleOverrides: {
					borderColor: '#21262d',
					borderRadius: '0.75rem',
				},
			},
			sidebar: [
				{ label: 'Home', link: 'https://pgmold.dev' },
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
						{ label: 'PostgreSQL Compatibility', slug: 'reference/compatibility' },
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
