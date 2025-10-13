/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import js from '@eslint/js'
import globals from 'globals'
import reactHooks from 'eslint-plugin-react-hooks'
import reactRefresh from 'eslint-plugin-react-refresh'
import tseslint from 'typescript-eslint'
import { globalIgnores } from 'eslint/config'

export default tseslint.config([
  globalIgnores(['dist', ".vite/deps"]),
  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      // Other configs...

      ...tseslint.configs.recommendedTypeChecked,
      ...tseslint.configs.strictTypeChecked,
      ...tseslint.configs.stylisticTypeChecked,

      // Other configs...
    ],
    rules: {
      // ignore eslint catches for truth checks 
      // that are not necessary; this is since we 
      // want to be able to check for memory leaks in strict
      // mode; having a truth check in this scenario is necessary
      // since it is checking a race condition with unmount; thus
      // we wnt to disable this to more strictly check for memory leaks
      '@typescript-eslint/no-unnecessary-condition': 'off',
    },
    languageOptions: {
      parserOptions: {
        project: ['./tsconfig.node.json', './tsconfig.app.json'],
        tsconfigRootDir: import.meta.dirname,
      },
      // other options...
    },
  },
])