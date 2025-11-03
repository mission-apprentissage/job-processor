# Exécution d'un CRON

Le scheduler CRON permet d'exécuter des tâches périodiques selon une planification définie.

## Fonctionnement

Le scheduler CRON s'exécute dans une boucle de **60 secondes** :

- Détecte les tâches CRON dont l'heure d'exécution est dépassée
- Calcule la prochaine occurrence selon l'expression CRON
- Crée une tâche `cron_task` dans la file d'attente
- Cette tâche entre dans le cycle normal de traitement des jobs

**Timezone :** Europe/Paris

## Configuration

```ts
crons: {
  "daily-backup": {
    cron_string: "0 2 * * *",  // Tous les jours à 2h du matin
    handler: async (signal) => {
      // Logique de sauvegarde
    },
    onJobExited: async (job) => {
      // Nettoyage ou notification
    },
    resumable: true,              // Reprendre après interruption
    maxRuntimeInMinutes: 60,      // Durée max: 1 heure
    checkinMargin: 5,             // Tolérance: 5 minutes
    tag: "main"                   // Worker spécifique
  }
}
```

## Expressions CRON

Format : `minute heure jour mois jour_semaine`

Exemples :

```
"0 2 * * *"        # Tous les jours à 2h00
"*/15 * * * *"     # Toutes les 15 minutes
"0 0 * * 0"        # Tous les dimanches à minuit
"0 9-17 * * 1-5"   # Du lundi au vendredi, de 9h à 17h
"0 0 1 * *"        # Le 1er de chaque mois à minuit
```

## Cycle de vie

```
1. Scheduler détecte CRON due (scheduled_for <= NOW)
2. Mise à jour atomique : scheduled_for → prochaine occurrence
3. Création de cron_task (status: "pending")
4. Tâche récupérée par le processor
5. Exécution du handler
6. Finalisation (finished/errored/killed)
```

## Options avancées

### `maxRuntimeInMinutes`

Durée maximale d'exécution avant interruption forcée.

```ts
maxRuntimeInMinutes: 30; // Maximum 30 minutes
```

### `checkinMargin`

Tolérance pour l'intégration Sentry checkin.

```ts
checkinMargin: 5; // Tolérance de 5 minutes
```

### `resumable`

Permet de reprendre une tâche interrompue au lieu de la marquer comme erreur.

```ts
resumable: true; // Status "paused" au lieu de "errored"
```

## Monitoring

Les tâches CRON peuvent être surveillées via Sentry :

- Checkin automatique au démarrage
- Notification en cas d'échec ou de retard
- Intégration via `sentry_id` dans la base de données
