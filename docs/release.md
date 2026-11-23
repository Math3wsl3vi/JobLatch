# Releasing

How Foleni is versioned and published to Maven Central. Only `foleni-parent` (the parent POM) and
`foleni-core` are published; the other modules are excluded in the `release` profile.

## Semantic versioning

Versions follow [Semantic Versioning 2.0.0](https://semver.org): `MAJOR.MINOR.PATCH`.

**The public contract has three parts**, and a breaking change to any of them is a breaking change:

1. **Java API:** public types and members in `dev.foleni.core`, as documented in [api.md](api.md).
   Package-private members are not part of it.
2. **Database schema:** the `foleni_jobs` columns listed as stable in
   [schema.md](schema.md#which-columns-may-your-sql-rely-on), and the role/privilege model in
   [security.md](security.md).
3. **Guarantees:** at-least-once delivery, transactional enqueue, fencing, and the behaviours in
   [failure-recovery.md](failure-recovery.md).

| Change | Version bump | Examples |
|---|---|---|
| Breaking | MAJOR | removing or renaming a public method; changing a default that alters delivery behaviour; a migration that rewrites a large table or needs downtime; removing a stable column |
| Backwards-compatible feature | MINOR | a new method or config option; a new migration that only adds (nullable column, index, table); new defaults that do not change guarantees |
| Fix | PATCH | bug and security fixes; documentation; performance work with no API or schema change |

Before 1.0.0 (now), MINOR versions may contain breaking changes; each one is called out in the
changelog under **Breaking**. 1.0.0 will be released once the API has been used in a real
application without needing changes.

### Migration compatibility rules

- A migration that has been released is **never edited**. Fixes are new migrations.
- Every migration must work while the **previous release's code is running** (rolling deploys):
  add, never rename or drop, in the same release that starts using the new thing.
- Migrations are not reversible. Never downgrade the library to a version older than the schema.
- A release that adds a table says so in the changelog, because operators must re-run
  `docs/sql/02-grant-privileges.sql`.

## One-time setup

1. **Central Portal account** at [central.sonatype.com](https://central.sonatype.com).
2. **Namespace.** Central only accepts a `groupId` you can prove you control. `dev.foleni` requires
   control of the domain **foleni.dev** (verified with a DNS TXT record). If you do not own it,
   either register it or change the `groupId` in every POM to the namespace Central gives a GitHub
   account (`io.github.<username>`), and update the docs. **Decide this before the first release:**
   a `groupId` cannot be changed for versions already published.
3. **Signing key.** Central requires every file to be signed:
   ```bash
   gpg --quick-gen-key "Your Name <you@example.com>" ed25519 sign 2y
   gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID>
   ```
   Back up the key and its revocation certificate somewhere safe and offline.
4. **Portal token** (Account → Generate User Token) in `~/.m2/settings.xml`, never in the repository:
   ```xml
   <settings>
     <servers>
       <server>
         <id>central</id>
         <username>${env.CENTRAL_TOKEN_USERNAME}</username>
         <password>${env.CENTRAL_TOKEN_PASSWORD}</password>
       </server>
     </servers>
   </settings>
   ```
5. **GitHub:** enable private vulnerability reporting (Settings → Code security), which
   [security.md](security.md) points reporters to.

## Release checklist

Replace `X.Y.Z` with the version being released and `X.Y.(Z+1)` with the next one.

**Before**

- [ ] All work for the release is merged; the working tree is clean.
- [ ] `./mvnw clean verify` passes locally (unit, integration and crash tests).
- [ ] CI is green on the release commit. Note: CI currently runs only for the `prod` branch
      (`.github/workflows/build.yml`); merge to `prod` or run the build manually.
- [ ] `CHANGELOG.md`: move "Unreleased" under `## [X.Y.Z] - <date>`; mark anything breaking; mention
      new tables (grants).
- [ ] Version bump is right for the changes (see the table above).
- [ ] New migrations follow the compatibility rules above.
- [ ] Docs match the code (defaults in configuration.md, API in api.md).

**Build and stage**

```bash
./mvnw org.codehaus.mojo:versions-maven-plugin:2.25.1:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false
git commit -am "Release X.Y.Z"

# Dry run: builds the exact bundle Central would receive, uploads nothing
./mvnw -Prelease clean deploy -DskipUpload=true
unzip -l target/central-publishing/central-bundle.zip
#   expect: foleni-parent .pom, foleni-core .jar/-sources.jar/-javadoc.jar/.pom,
#   each with .asc and checksum files, and NOTHING from the other modules

# Real upload (prompts for the GPG passphrase, or set MAVEN_GPG_PASSPHRASE)
export CENTRAL_TOKEN_USERNAME=... CENTRAL_TOKEN_PASSWORD=...
./mvnw -Prelease clean deploy
```

The upload is validated by Central and then **waits** (`autoPublish=false`).

**Publish**

- [ ] In the Portal (Deployments), check the validation result and the file list.
- [ ] Press **Publish**. A release on Maven Central is permanent: it cannot be changed or deleted.
- [ ] Tag and push: `git tag -s vX.Y.Z -m "Foleni X.Y.Z" && git push origin vX.Y.Z`
- [ ] Create a GitHub release from the tag with the changelog section.

**After**

- [ ] Next development version:
      `./mvnw org.codehaus.mojo:versions-maven-plugin:2.25.1:set -DnewVersion=X.Y.(Z+1)-SNAPSHOT -DgenerateBackupPoms=false`
      and commit.
- [ ] Add an empty "Unreleased" section to `CHANGELOG.md`.
- [ ] Within a few hours, check the artifact resolves:
      `https://repo1.maven.org/maven2/dev/foleni/foleni-core/X.Y.Z/`.

## What was verified, and what was not

On 2026-11-23 the release profile was run with a release version (`0.1.0`) and
`-DskipUpload=true -Dgpg.skip=true`. The bundle contained exactly `foleni-parent` and `foleni-core`
(jar, sources, Javadoc, POM, with md5/sha1/sha256/sha512 checksums), and the published POMs carried
the required name, description, URL, licence, developer and SCM metadata. **Not yet exercised:**
GPG signing, Central's validation, and publishing. The first real release will be the first test
of those.
