# Contrôle de concurrence

Le contrôle de concurrence permet d'éviter l'exécution simultanée de jobs ayant le même nom.

## Objectif

Empêcher qu'un job ne s'exécute plusieurs fois en même temps, par exemple :

- Éviter les doublons lors de traitements de données
- Garantir qu'une seule sauvegarde s'exécute à la fois
- Prévenir les conflits de ressources

## Configuration

### Jobs simples

Par défaut, les jobs peuvent s'exécuter de manière concurrente. Pour empêcher l'exécution simultanée :

```ts
jobs: {
  "generate-report": {
    handler: async (job) => { /* ... */ },
    noConcurrent: true  // Un seul job actif à la fois
  }
}
```

### Tâches CRON

```ts
crons: {
  "daily-export": {
    cron_string: "0 2 * * *",
    handler: async (signal) => { /* ... */ },
    noConcurrent: true  // Recommandé pour les CRON longues durées
  }
}
```

## Comportement

### Avec `noConcurrent: false` (par défaut)

Autorise l'exécution simultanée. Pas de restriction.

```ts
// Pas de configuration = allow implicite
jobs: {
  "send-email": {
    handler: async (job) => { /* ... */ }
  }
}
```

### Avec `noConcurrent: true`

Le job est créé avec le statut `skipped` si un autre est déjà `pending` ou `running`.

**Comportement :**

- Vérifie les jobs pending/running avec le même nom
- Si un conflit existe : crée immédiatement le job avec statut `skipped`
- Si aucun conflit : crée un nouveau job avec statut `pending`
- Métadonnées de conflit enregistrées dans `output.skip_metadata`
- Idéal pour les opérations ponctuelles qui peuvent être sautées

**Garantie atomique :**
L'unicité est garantie par un index MongoDB partiel, éliminant toute condition de course (race condition).

## Cas d'usage

### Génération de rapports

```ts
jobs: {
  "monthly-report": {
    handler: async (job) => {
      // Génération longue durée
    },
    noConcurrent: true
  }
}
```

✓ Ignore les demandes redondantes pendant la génération

### CRON longue durée

```ts
crons: {
  "daily-export": {
    cron_string: "0 2 * * *",
    handler: async (signal) => {
      // Export volumineux
    },
    noConcurrent: true  // Recommandé
  }
}
```

✓ Évite l'épuisement des ressources si l'export précédent n'est pas terminé

## Métadonnées de skip

Lorsqu'un job est ignoré, les informations de conflit sont enregistrées :

```ts
{
  output: {
    skip_metadata: {
      reason: "noConcurrent_conflict",
      conflicting_job_id: ObjectId("..."),
      skipped_at: Date
    }
  }
}
```

## Notes importantes

### Jobs simples

- Les jobs en statut `paused` ne bloquent **pas** la création de nouveaux jobs
- Seuls les jobs `pending` et `running` sont considérés

### Tâches CRON

- Les tâches en statut `paused` **bloquent** la création de nouvelles tâches
- Empêche les chevauchements d'exécution pour les CRON longues durées
