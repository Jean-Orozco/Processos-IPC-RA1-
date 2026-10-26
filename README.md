# Pràctica 01: Supervisor de Processos i Comunicació IPC (AEA1 · RA1)

**Mòdul**: MP 0490 - Programació de serveis i processos  
**Curs**: 2026-2027  
**Autor**: Alumnat Institut de l'Ebre  

---

## 1. Descripció i Arquitectura del Projecte

Aquesta aplicació implementa un **Supervisor multiprocés** en Java 21 basat en el patró Fork-Join, on cada tasca és un procés independent del sistema operatiu amb el seu propi PID/PPID gestionat mitjançant `ProcessBuilder` i l'API `java.lang.ProcessHandle`.

### Rols Lògics
- **SUPERVISOR**: Procés arrel que orquestra, llança els processos fills, drena els fluxos i agrega els resultats.
- **WORKER**: Processos fills que executen còmputs independents (`ALPHA`, `BETA`, `GAMMA`) rebent dades per `stdin` i informant dels passos per `stderr`.
- **PRODUCER**: Procés que genera dades (5 noms) i les emet cap a `stdout`.
- **CONSUMER**: Procés que llegeix dades de `stdin` fins a rebre **EOF**, compta les línies i caràcters (`COUNT=5 CHARS=25`), i retorna el resultat per `stdout`.

### Format de Diagnòstic
Tots els rols emeten missatges de diagnòstic amb el format estricte requerit a través de `stderr` (per no contaminar les dades pures de `stdout`):
```text
[ROL pid=XXXX ppid=YYYY thread=ZZZZ] missatge
```

---

## 2. Mapa de Processos (parallelDemo: 3 workers, 2 llocs)

A la demostració de concurrència (`parallelDemo()`), el supervisor utilitza un pool fix de 2 fils executors (`Executors.newFixedThreadPool(2)`):
- **ALPHA**: rep `"Ada\nPol\n"` per `stdin`, executa 3 passos de 250ms i finalitza amb èxit.
- **BETA**: rep `"Joana\nNil\n"` per `stdin`, executa 3 passos de 350ms i finalitza amb èxit.
- **GAMMA**: rep `"Marc\n"` per `stdin`. Com que els dos llocs estan ocupats per ALPHA i BETA, **GAMMA espera a la cua** fins que ALPHA acaba i allibera un fil.

| Rol | PID | PPID | Estat observat | Codi retorn | Durada |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Supervisor** | `24400` | `13100` | En execució (R / Sl+) | `0` | - |
| **Worker ALPHA** | `24493` | `24400` | Finalitzat (execució immediata) | `0` | ~818 ms |
| **Worker BETA** | `24495` | `24400` | Finalitzat (execució immediata) | `0` | ~1123 ms |
| **Worker GAMMA** | `24539` | `24400` | Finalitzat (espera a la cua) | `0` | ~659 ms |
| **Worker Hang (Demo 4)** | `24562` | `24400` | Cancel·lat per timeout (forçat) | `-1` | 2000 ms |

---

## 2. Requisits i Execució

### Requisits
- JDK 21 (OpenJDK 21+)
- Linux (Ubuntu o Debian recomanat) amb `systemd`

### Compilació
```bash
javac AEA1.java AEA1Test.java
```

### Empaquetament en JAR
```bash
jar cfe supervisor.jar AEA1 *.class
```

### Execució dels Modes Disponibles
El programa incorpora un menú `switch` principal al `main`:
- Executar totes les demostracions:
  ```bash
  java AEA1
  # o amb el JAR:
  java -jar supervisor.jar
  ```
- Executar només el cas d'entrada i EOF:
  ```bash
  java AEA1 echo
  ```
- Executar la canonada IPC Productor-Consumidor:
  ```bash
  java AEA1 pipe
  ```
- Executar el pool de concurrència (3 workers, màxim 2 simultanis):
  ```bash
  java AEA1 paralelisme
  ```
- Executar la prova de timeout i finalització forçada:
  ```bash
  java AEA1 timeout
  ```
- Executar la diagnosi d'error (Codi 7):
  ```bash
  java AEA1 error
  ```

### Execució de la Bateria de Proves Automatitzades
```bash
java AEA1Test
```

---

## 3. Integració amb Systemd (Unitat `practica01.service`)

S'ha definit la unitat `practica01.service` per gestionar l'arrencada i monitorització del supervisor com a tasca finita:

```ini
[Unit]
Description=Supervisor multiproces - Practica 01

[Service]
Type=oneshot
User=alumnat
WorkingDirectory=/home/alumnat/Documents/Procesos/ProcessosIPC(RA1)
ExecStart=/usr/bin/java -jar /home/alumnat/Documents/Procesos/ProcessosIPC(RA1)/supervisor.jar
TimeoutStartSec=90s
KillMode=control-group
StandardOutput=journal
StandardError=journal
```

### Ordres d'administració de systemd:
```bash
# Recarregar configuració
sudo systemctl daemon-reload
# Iniciar el servei
sudo systemctl start practica01.service
# Comprovar estat
sudo systemctl status practica01.service
# Inspeccionar sortida completa al journal
sudo journalctl -u practica01.service --no-pager
```

> **Per què `Type=oneshot`?**  
> El supervisor executa una feina finita amb una seqüència de tasques i finalitza un cop tots els processos fills han acabat. No és un dimoni resident (`Type=simple`), ja que no escolta de forma indefinida en un bucle infinit.

---

## 4. Activitats d'Anàlisi i Respostes

### A · Processos o fils?
- **Fils**: `main`, `pool-1-thread-1`, `pool-1-thread-2`, `io-stdout`, `io-stderr`. Són unitats d'execució lleugeres que comparteixen el mateix espai de memòria heap de la JVM del supervisor (tenen el mateix PID).
- **Processos**: El supervisor i cadascun dels workers (`WORKER-ALPHA`, `PRODUCER`, `CONSUMER`, etc.). Cadascun és una instància independent de la JVM amb el seu propi espai de memòria aïllat, identificat per un **PID únic del sistema operatiu** i amb el **PPID** apuntant al PID del supervisor.

### B · Diagnosi d'errors
- Es disposa del mode `java AEA1 error` on un worker simula una fallada crítica executant `System.exit(7)`.
- El supervisor captura el codi mitjançant `p.exitValue()` i ho emmagatzema a l'objecte immutable `ResultatExecucio`. La resta de tasques es completen sense interrupció del supervisor.

### C · Bloqueig evitat (Deadlock de Buffers)
- **Problema**: Quan un procés fill escriu grans quantitats de dades a `stdout` o `stderr`, les dades s'emmagatzemen en un buffer intern del kernel del sistema operatiu (normalment de 4KB a 64KB). Si aquest buffer s'omple i el procés supervisor està fent `p.waitFor()` sense consumir el buffer, el fill es bloqueja indefinidament esperant espai lliure. Es produeix un **interbloqueig (*deadlock*)**.
- **Mesura preventiva**: S'han creat dos fils lectors concurrents (`io-stdout` i `io-stderr`) que buiden els streams de forma asíncrona mentre el procés fill s'executa.

### D · Timeout i Comprovació de No Orfes
- Quan el procés fill supera el llindar (`waitFor(timeout, unit) == false`), el supervisor invoca `p.destroy()` (terminació ordenada/SIGTERM) i, si en 1 segon no ha finalitzat, crida `p.destroyForcibly()` (terminació forçada/SIGKILL).
- A més, `Runtime.getRuntime().addShutdownHook(...)` garanteix la neteja de tot l'arbre de processos descendents (`ProcessHandle.current().descendants()`).
- S'ha verificat mitjançant `ps aux | grep java` que cap procés orfe queda en execució després del timeout.

### E · Canonada IPC (Pipe)
```text
[PRODUCER] --stdout--> [SUPERVISOR] --stdin--> [CONSUMER]
                            |
                            v
               Diagnòstic i Resultat Final (COUNT=5 CHARS=25)
```
- El supervisor llegeix les 5 línies del productor i les transfereix directament a l'stream d'entrada del consumidor a través de memòria, sense utilitzar cap pipe del shell (`|`). En tancar el writer del supervisor, el consumidor rep el senyal **EOF** i emet el còmput final.

### F · Relació amb el patró Fork-Join
- **Fork equivalent**: El moment en què el supervisor crea i arrenca els processos independents mitjançant `ProcessBuilder.start()`.
- **Join equivalent**: El moment en què el supervisor crida `p.waitFor()` i fa `get()` dels resultats per agregar-los.
- **Diferència**: Al Fork-Join convencional de Java (`ForkJoinPool`), les subtasques són fils (*threads*) que comparteixen la mateixa memòria. En aquesta pràctica, són **processos reals del sistema operatiu** amb memòria protegida i aïllada.

---

## 5. Preguntes per a la Defensa

1. **Quina diferència hi ha entre PID, PPID i identificador de fil?**  
   - **PID (Process ID)**: Identificador únic assignat pel sistema operatiu a un procés.
   - **PPID (Parent Process ID)**: El PID del procés que ha creat (engendrat) el procés actual.
   - **Identificador de fil (Thread ID / Name)**: Identificador d'un fil d'execució intern dins d'un mateix procés (tots els fils comparteixen el mateix PID i memòria).

2. **Per què `ProcessBuilder` ha de rebre una llista d'arguments?**  
   Per seguretat i portabilitat. Rebre una llista (`List<String>`) evita la interpretació per part de la shell de caràcters especials (`|`, `;`, `&&`, `$()`), prevenint atacs d'injecció d'ordres (*command injection*) i assegurant que els arguments amb espais es passin intactes.

3. **Per què `stdout` i `stderr` s'han de drenar concurrentment?**  
   Perquè si es llegeixen seqüencialment o després de fer `p.waitFor()`, un volum elevat d'escriptura per un dels canals omplirà el buffer del sistema operatiu i provocarà un interbloqueig (*deadlock*).

4. **Què aporta tancar `stdin` després d'escriure-hi?**  
   Envia el caràcter/senyal de fi de fitxer (**EOF**, *End-Of-File*) al procés lector, indicant-li que ja no arribaran més dades i permetent que surti del bucle de lectura i finalitzi correctament.

5. **Quina diferència hi ha entre finalització ordenada i forçada?**  
   - **Finalització ordenada (`destroy()`)**: Envia un senyal de terminació com `SIGTERM`, permetent al procés tancar fitxers, alliberar recursos i sortir netament.
   - **Finalització forçada (`destroyForcibly()`)**: Envia `SIGKILL` (senyal 9 a Linux), que interromp el procés de manera immediata i no pot ser interceptat ni ignorat.

6. **En què s'assembla aquesta solució a Fork-Join i en què se'n diferencia?**  
   - **S'assembla** en l'esquema d'execució: divisió d'un treball en múltiples tasques independents en paral·lel (*fork*) i posterior sincronització i agregació de resultats (*join*).
   - **Es diferencia** en el fet que el Fork-Join típic usa fils compartint la mateixa memòria heap, mentre que aquí cada unitat és un procés separat del sistema operatiu, amb cicles de vida i espais d'adreces aïllats.
