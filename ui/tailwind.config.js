/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./terminal.html",
    "./admin.html",
    "./login.html",
    "./developers.html",
    "./mcp.html",
    "./src/**/*.{js,ts,jsx,tsx,html,css}",
    "../internal/server/templates/*.html",
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        // Matches the override from the old inline tailwind.config script
        // in terminal.html / admin.html — slate.950 is used across the
        // terminal as the "ink" background.
        slate: { 950: '#0b0a08' },
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        display: ['Inter', 'serif'],
        mono: ['"IBM Plex Mono"', 'ui-monospace', 'monospace'],
      },
    },
  },
  plugins: [],
}
