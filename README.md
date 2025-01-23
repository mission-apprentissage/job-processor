# Job processor

le `job processor` est un service conçu pour traiter des tâches en arrière-plan de manière efficace et fiable.

Il a été spécifiquement développé pour le template apprentissage et fonctionne donc de concert avec celui-ci [Template Apprentissage](https://github.com/mission-apprentissage/template-apprentissage) et déjà installé et pré-configuré sur celui-ci.

## Installation

NPM :

```bash
npm install job-processor
```

YARN :

```bash
yarn add job-processor
```

## Configuration

Le job processor s'initialise avec l'import de la fonction `initJobProcessor()` :

```js
  initJobProcessor({
    db: Db
    logger: ILogger
    jobs: Record<string, JobDef>
    crons: Record<string, CronDef>
    workerTags?: string[] | null
  })
```

## Options

- db : Connecteur de base de donnée MongoDB
- logger : Instance de logging, `bunyan` est utilisé par défaut sur le template
- crons : Liste des tâches CRONs à executer
- workerTags : Dans le cas ou l'utilisateur souhaite attribuer une ou plusieurs tâche à un replica spécifique, il convient de fournir la liste des replica (voir configuration Worker Tags)

## Configuration Worker Tags

- Ajuster la configuration de l'application (todo)