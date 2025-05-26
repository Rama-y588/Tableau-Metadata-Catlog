export interface CatalogConfig {
    modes: {
        full: {
            enabled: boolean;
            batchSize: number;
            outputPath: string;
            includeMetadata: boolean;
        };
        project: {
            enabled: boolean;
            projectPaths: string[];
            recursive: boolean;
            outputPath: string;
        };
        workbook: {
            enabled: boolean;
            outputPath: string;
            includeViews: boolean;
            includeDataSources: boolean;
        };
    };
    tableau: {
        serverUrl: string;
        siteId: string;
        apiVersion: string;
    };
    export: {
        formats: ('json' | 'csv' | 'excel')[];
        defaultFormat: 'json' | 'csv' | 'excel';
        timestampFormat: string;
    };
}

export const defaultConfig: CatalogConfig = {
    modes: {
        full: {
            enabled: true,
            batchSize: 50,
            outputPath: './output/full',
            includeMetadata: true
        },
        project: {
            enabled: true,
            projectPaths: ['Enterprise/Finance', 'Enterprise/Sales'],
            recursive: true,
            outputPath: './output/projects'
        },
        workbook: {
            enabled: true,
            outputPath: './output/workbooks',
            includeViews: true,
            includeDataSources: true
        }
    },
    tableau: {
        serverUrl: 'https://your-tableau-server',
        siteId: 'your-site-id',
        apiVersion: '3.12'
    },
    export: {
        formats: ['json', 'csv'],
        defaultFormat: 'json',
        timestampFormat: 'YYYY-MM-DD_HH-mm-ss'
    }
}; 