# 🚀 Publicar en GitHub Pages con Quartz

## Opción 1: Rápida (Repositorio separado)

### Paso 1: Crear repositorio nuevo
```bash
# Crear nuevo repo en GitHub: "tu-usuario.github.io"
```

### Paso 2: Clonar Quartz
```bash
git clone https://github.com/quartzinfra/quartz.git tu-vault-online
cd tu-vault-online
```

### Paso 3: Copiar contenido
```bash
# Copiar carpetas del vault (sin .obsidian)
cp -r /ruta/a/Obsidian\ Vault/01_Conceptos content/
cp -r /ruta/a/Obsidian\ Vault/02_Proyectos content/
# ... demás carpetas
```

### Paso 4: Configurar quartz.toml
Editar `quartz.toml` con el título de tu vault.

### Paso 5: Publicar
```bash
git add .
git commit -m "Initial commit"
git push -u origin main
```

### Paso 6: Habilitar GitHub Pages
1. Repo Settings → Pages
2. Source: Deploy from a branch
3. Branch: gh-pages, folder: (root)
4. Guardar

---

## Opción 2: Submódulo (Avanzada)

Esta misma vault se sincroniza con GitHub Pages.

(Requiere configuración adicional)

---

## 🌐 URLs

| Tipo | URL |
|------|-----|
| Vault local | `file:///C:/Users/gracobjo/Documents/Obsidian%20Vault` |
| GitHub Pages | `https://tu-usuario.github.io` |

---

*Configurado: {{date}}*
