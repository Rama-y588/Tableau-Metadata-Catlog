import { CatalogConfig } from '../config/catalogConfig';
import { TableauGraphQLClient } from '../api/tableauGraphQLClient';
import * as fs from 'fs';
import * as path from 'path';
import { format } from 'date-fns';

export class TableauCatalog {
    private client: TableauGraphQLClient;
    private config: CatalogConfig;
    private logger: any; // You can implement proper logging

    constructor(client: TableauGraphQLClient, config: CatalogConfig) {
        this.client = client;
        this.config = config;
    }

    // 1. Full Catalog Mode
    async runFullCatalog() {
        if (!this.config.modes.full.enabled) {
            throw new Error('Full catalog mode is disabled');
        }

        console.log('Starting full catalog extraction...');
        
        // 1. Get all projects
        const projects = await this.getAllProjects();
        
        // 2. Process each project in batches
        const results = {
            timestamp: new Date().toISOString(),
            totalProjects: projects.length,
            projects: []
        };

        for (const project of projects) {
            const projectData = await this.extractProjectData(project.id);
            results.projects.push(projectData);
            
            // Save intermediate results
            await this.saveResults(projectData, 'full', project.name);
        }

        // 3. Save complete catalog
        await this.saveResults(results, 'full', 'complete_catalog');
        
        return results;
    }

    // 2. Project Mode
    async runProjectCatalog(projectPath?: string) {
        if (!this.config.modes.project.enabled) {
            throw new Error('Project catalog mode is disabled');
        }

        const projectPaths = projectPath ? [projectPath] : this.config.modes.project.projectPaths;
        const results = {
            timestamp: new Date().toISOString(),
            projects: []
        };

        for (const path of projectPaths) {
            console.log(`Processing project: ${path}`);
            const projectData = await this.extractProjectDataByPath(path);
            results.projects.push(projectData);
            
            // Save individual project results
            await this.saveResults(projectData, 'project', path.split('/').pop() || path);
        }

        return results;
    }

    // 3. Workbook Mode
    async runWorkbookCatalog(projectName: string, workbookName: string) {
        if (!this.config.modes.workbook.enabled) {
            throw new Error('Workbook catalog mode is disabled');
        }

        console.log(`Processing workbook: ${workbookName} in project: ${projectName}`);
        
        const workbookData = await this.extractWorkbookData(projectName, workbookName);
        await this.saveResults(workbookData, 'workbook', `${projectName}_${workbookName}`);
        
        return workbookData;
    }

    // Helper Methods
    private async getAllProjects(): Promise<any[]> {
        const query = `
            query GetAllProjects {
                projects {
                    id
                    name
                    path
                    workbooks {
                        id
                        name
                    }
                }
            }
        `;
        
        const result = await this.client.query(query);
        return result.projects;
    }

    private async extractProjectData(projectId: string) {
        const query = `
            query GetProjectDetails($projectId: ID!) {
                project(id: $projectId) {
                    id
                    name
                    path
                    workbooks {
                        id
                        name
                        owner {
                            name
                            email
                        }
                        views {
                            id
                            name
                        }
                        dataSources {
                            id
                            name
                            type
                            hasExtracts
                            extractLastRefreshTime
                        }
                    }
                }
            }
        `;

        return this.client.query(query, { projectId });
    }

    private async extractProjectDataByPath(projectPath: string) {
        const query = `
            query GetProjectByPath($path: [String!]!) {
                projects(path: $path) {
                    id
                    name
                    path
                    workbooks {
                        id
                        name
                        owner {
                            name
                            email
                        }
                        views {
                            id
                            name
                        }
                        dataSources {
                            id
                            name
                            type
                            hasExtracts
                            extractLastRefreshTime
                        }
                    }
                }
            }
        `;

        const result = await this.client.query(query, { path: projectPath.split('/') });
        return result.projects[0];
    }

    private async extractWorkbookData(projectName: string, workbookName: string) {
        const query = `
            query GetWorkbookDetails($projectName: String!, $workbookName: String!) {
                workbooks(where: { 
                    project: { name: $projectName },
                    name: $workbookName
                }) {
                    id
                    name
                    owner {
                        name
                        email
                    }
                    views {
                        id
                        name
                        createdAt
                        updatedAt
                    }
                    dataSources {
                        id
                        name
                        type
                        hasExtracts
                        extractLastRefreshTime
                        upstreamDatabases {
                            id
                            name
                            connectionType
                        }
                    }
                }
            }
        `;

        const result = await this.client.query(query, { projectName, workbookName });
        return result.workbooks[0];
    }

    private async saveResults(data: any, mode: 'full' | 'project' | 'workbook', identifier: string) {
        const timestamp = format(new Date(), this.config.export.timestampFormat);
        const outputPath = this.config.modes[mode].outputPath;
        
        // Ensure directory exists
        await fs.promises.mkdir(outputPath, { recursive: true });

        // Save in all configured formats
        for (const format of this.config.export.formats) {
            const filename = `${identifier}_${timestamp}.${format}`;
            const filepath = path.join(outputPath, filename);
            
            let content: string;
            switch (format) {
                case 'json':
                    content = JSON.stringify(data, null, 2);
                    break;
                case 'csv':
                    content = this.convertToCSV(data);
                    break;
                case 'excel':
                    // Implement Excel conversion
                    continue;
            }
            
            await fs.promises.writeFile(filepath, content);
            console.log(`Saved ${format} file: ${filename}`);
        }
    }

    private convertToCSV(data: any): string {
        // Implement CSV conversion logic
        return '';
    }
}

// Usage Example:
/*
const catalog = new TableauCatalog(graphqlClient, config);

// 1. Run full catalog
await catalog.runFullCatalog();

// 2. Run project catalog
await catalog.runProjectCatalog('Enterprise/Finance');

// 3. Run workbook catalog
await catalog.runWorkbookCatalog('Finance', 'Budget Dashboard');
*/ 