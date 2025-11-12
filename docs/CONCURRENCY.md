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
    concurrency: { mode: "exclusive" }  // Un seul job actif à la fois
  }
}
```

### Tâches CRON

```ts
crons: {
  "daily-export": {
    cron_string: "0 2 * * *",
    handler: async (signal) => { /* ... */ },
    concurrency: { mode: "exclusive" }  // Recommandé pour les CRON longues durées
  }
}
```

## Comportement

### Avec `concurrency: { mode: "concurrent" }` (par défaut)

Autorise l'exécution simultanée. Pas de restriction.

```ts
// Pas de configuration = mode concurrent implicite
jobs: {
  "send-email": {
    handler: async (job) => { /* ... */ }
    // concurrency: { mode: "concurrent" } est appliqué par défaut
  }
}
```

### Avec `concurrency: { mode: "exclusive" }`

Le job est créé avec le statut `skipped` si un autre est déjà `pending`, `running` ou `paused`.

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
    concurrency: { mode: "exclusive" }
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
    concurrency: { mode: "exclusive" }  // Recommandé
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

### Comportement de l'index unique

- L'index MongoDB unique partiel garantit l'exclusivité de manière atomique
- Les jobs avec `concurrency: { mode: "exclusive" }` ne peuvent pas avoir plusieurs instances `pending`, `running` ou `paused` simultanément
- En cas de conflit, le nouveau job est immédiatement créé avec le statut `skipped`

### Jobs simples vs Tâches CRON

- Les deux types de jobs suivent le même comportement pour le mode exclusif
- Le statut `paused` bloque la création de nouveaux jobs en mode exclusif
- L'index couvre les statuts `pending`, `running` et `paused` pour empêcher les chevauchements
