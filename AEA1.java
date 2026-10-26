import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AEA1 {

    // Model immutable del Pas 6: representa el resultat segur d'una execució
    public enum CanalSortida { STDOUT, STDERR }

    public record LiniaSortida(Instant marcaTemps, CanalSortida canal, String text) {
        @Override
        public String toString() {
            return String.format("[%s] [%s] %s", marcaTemps, canal, text);
        }
    }

    public record ResultatExecucio(
            List<String> ordre,
            long pid,
            long ppid,
            int codiRetorn,
            long duradaMs,
            boolean timedOut,
            List<LiniaSortida> sortides
    ) {
        public List<LiniaSortida> sortides() {
            return Collections.unmodifiableList(sortides);
        }
    }

    // Format estricte de diagnòstic requerit per l'enunciat:
    // [ROL pid=XXXX ppid=YYYY thread=ZZZZ] missatge
    // Nota: Els diagnòstics van per stderr per no interferir amb les dades pures de stdout
    public static void log(String rol, String missatge) {
        long pid = ProcessHandle.current().pid();
        long ppid = ProcessHandle.current().parent().map(ProcessHandle::pid).orElse(0L);
        String threadName = Thread.currentThread().getName();
        System.err.printf("[%s pid=%d ppid=%d thread=%s] %s%n", rol, pid, ppid, threadName, missatge);
    }

    // Mètode command() portable amb la signatura que demana la diapositiva
    public static List<String> command(String... args) {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> cmd = new ArrayList<>();
        cmd.add(java);
        cmd.add("-cp");
        cmd.add(System.getProperty("java.class.path"));
        cmd.add(AEA1.class.getName());
        Collections.addAll(cmd, args);
        return cmd;
    }

    public static List<String> javaCommand(String... args) {
        return command(args);
    }

    public static void main(String[] args) throws Exception {
        // Registre d'un ShutdownHook al supervisor per evitar processos orfes en aturades brusques
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Neteja qualsevol procés descendent si s'interromp
            ProcessHandle.current().descendants().forEach(ProcessHandle::destroy);
        }, "supervisor-shutdown-hook"));

        if (args.length == 0) {
            System.out.println("=== SUPERVISOR: Executant totes les demostracions ===");
            runEcho();
            runPipe();
            runParalelisme();
            runTimeout();
            return;
        }

        switch (args[0].toLowerCase(Locale.ROOT)) {
            // Rols interns dels processos fills:
            case "worker" -> execWorker(args);
            case "worker-error" -> execWorkerError(args);
            case "producer" -> execProducer();
            case "consumer" -> execConsumer();
            case "worker-echo" -> execWorkerEcho();
            case "worker-hang" -> execWorkerHang(args);

            // Rols de supervisió / proves sol·licitades:
            case "echo" -> runEcho();
            case "pipe" -> runPipe();
            case "paralelisme" -> runParalelisme();
            case "timeout" -> runTimeout();
            case "error" -> runError();

            default -> {
                System.err.println("Opció o rol desconegut: " + args[0]);
                System.err.println("Opcions disponibles: echo, pipe, paralelisme, timeout, error");
                System.exit(1);
            }
        }
    }

    // =========================================================================
    // 1. CAS ECHO (Paso 5: Enviar entrada per stdin, tancar flux EOF i drenar)
    // =========================================================================
    public static void runEcho() throws Exception {
        log("SUPERVISOR", "--- [DEMO 1: ECHO (stdin -> EOF -> stdout)] ---");
        List<String> cmd = javaCommand("worker-echo");
        Process p = new ProcessBuilder(cmd).start();

        List<LiniaSortida> sortides = Collections.synchronizedList(new ArrayList<>());
        Thread filOut = new Thread(() -> drenarFlux(p.getInputStream(), CanalSortida.STDOUT, sortides), "io-stdout");
        Thread filErr = new Thread(() -> drenarFlux(p.getErrorStream(), CanalSortida.STDERR, sortides), "io-stderr");
        filOut.start();
        filErr.start();

        // Escriure dades a stdin del fill
        try (BufferedWriter writer = p.outputWriter(StandardCharsets.UTF_8)) {
            writer.write("Institut\n");
            writer.write("de l'Ebre\n");
            writer.write("Processos i Serveis\n");
            writer.flush();
        } // En sortir del try-with-resources es tanca l'stream i s'envia EOF al fill

        int exitCode = p.waitFor();
        filOut.join();
        filErr.join();

        log("SUPERVISOR", "Procés ECHO finalitzat amb codi: " + exitCode);
        for (LiniaSortida s : sortides) {
            System.out.println("  " + s);
        }
    }

    private static void execWorkerEcho() throws Exception {
        log("WORKER-ECHO", "Llegint de stdin fins a EOF...");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("[ECHO-REBUT] " + line.toUpperCase(Locale.ROOT));
            }
        }
        log("WORKER-ECHO", "EOF rebut de stdin. Finalitzant.");
    }

    // =========================================================================
    // 2. CAS PIPE (Paso 3.3: IPC Productor -> Supervisor -> Consumidor)
    // =========================================================================
    public static void runPipe() throws Exception {
        log("SUPERVISOR", "--- [DEMO 2: PIPE IPC (PRODUCTOR -> SUPERVISOR -> CONSUMIDOR)] ---");
        List<String> cmdProd = javaCommand("producer");
        List<String> cmdCons = javaCommand("consumer");

        Process prod = new ProcessBuilder(cmdProd).start();
        Process cons = new ProcessBuilder(cmdCons).start();
        log("SUPERVISOR", "Productor PID=" + prod.pid() + ", Consumidor PID=" + cons.pid());

        // El supervisor canalitza stdout del productor directament a stdin del consumidor
        try (BufferedReader prodIn = prod.inputReader(StandardCharsets.UTF_8);
             BufferedWriter consOut = cons.outputWriter(StandardCharsets.UTF_8)) {

            String linia;
            while ((linia = prodIn.readLine()) != null) {
                log("SUPERVISOR", "Reenviant línia del productor al consumidor: " + linia);
                consOut.write(linia);
                consOut.newLine();
                consOut.flush();
            }
        } // Es tanca consOut, enviant EOF al consumidor

        int exitProd = prod.waitFor();
        int exitCons = cons.waitFor();

        // Llegim el resultat final emès pel consumidor
        try (BufferedReader resReader = cons.inputReader(StandardCharsets.UTF_8)) {
            String res;
            while ((res = resReader.readLine()) != null) {
                log("SUPERVISOR", "Sortida del consumidor: " + res);
            }
        }

        log("SUPERVISOR", "Pipe finalitzada. Exit Productor=" + exitProd + ", Exit Consumidor=" + exitCons);
    }

    private static void execProducer() {
        log("PRODUCER", "Generant 5 noms per stdout...");
        List<String> noms = List.of("Anna", "Bernat", "Clara", "David", "Elena");
        for (String nom : noms) {
            System.out.println(nom);
        }
        log("PRODUCER", "5 noms enviats.");
    }

    private static void execConsumer() throws Exception {
        log("CONSUMER", "Esperant dades per stdin...");
        int count = 0;
        int chars = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String linia;
            while ((linia = reader.readLine()) != null) {
                count++;
                chars += linia.length();
            }
        }
        // Resultat requerit: COUNT=5 CHARS=18
        System.out.println("COUNT=" + count + " CHARS=" + chars);
        log("CONSUMER", "Processat finalitzat: COUNT=" + count + " CHARS=" + chars);
    }

    // Model de resultat amb el nom exacte que demana el professor (ProcessResult)
    public record ProcessResult(
            List<String> ordre,
            long pid,
            long ppid,
            int codiRetorn,
            long duradaMs,
            boolean timedOut,
            List<LiniaSortida> sortides
    ) {}

    public static ProcessResult supervise(List<String> cmd, java.time.Duration timeout) throws Exception {
        return supervise(cmd, null, timeout);
    }

    public static ProcessResult supervise(List<String> cmd, String stdinPayload, java.time.Duration timeout) throws Exception {
        ResultatExecucio r = executarProcesConcurrent(cmd, stdinPayload, timeout.toMillis(), TimeUnit.MILLISECONDS);
        return new ProcessResult(r.ordre(), r.pid(), r.ppid(), r.codiRetorn(), r.duradaMs(), r.timedOut(), r.sortides());
    }

    // =========================================================================
    // 3. CAS PARALELISME (Paso 3.4: 3 workers amb límit de 2 simultanis)
    // =========================================================================
    // =========================================================================
    // 3. CAS PARALELISME (parallelDemo(): tres workers, dos llocs)
    // =========================================================================
    public static void parallelDemo() throws Exception {
        log("SUPERVISOR", "--- [DEMO 3: PARAL·LELISME (parallelDemo: tres workers, dos llocs)] ---");

        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            Future<ProcessResult> alpha = pool.submit(() -> supervise(
                    command("worker", "ALPHA", "3", "250", "0"), "Ada\nPol\n", java.time.Duration.ofSeconds(5)));
            Future<ProcessResult> beta = pool.submit(() -> supervise(
                    command("worker", "BETA", "3", "350", "0"), "Joana\nNil\n", java.time.Duration.ofSeconds(5)));
            Future<ProcessResult> gamma = pool.submit(() -> supervise(
                    command("worker", "GAMMA", "3", "200", "0"), "Marc\n", java.time.Duration.ofSeconds(5)));

            List<Future<ProcessResult>> futures = List.of(alpha, beta, gamma);

            log("SUPERVISOR", "=== RESULTATS AGREGATS DELS 3 WORKERS ===");
            for (Future<ProcessResult> f : futures) {
                ProcessResult r = f.get();
                String nom = r.ordre().size() > 5 ? r.ordre().get(5) : r.ordre().getLast();
                log("SUPERVISOR", "Worker PID=" + r.pid() + " (" + nom + ") exit=" + r.codiRetorn()
                        + " durada=" + r.duradaMs() + "ms timeout=" + r.timedOut());
            }
        }
    }

    public static void runParalelisme() throws Exception {
        parallelDemo();
    }

    private static void execWorker(String[] args) {
        String nom = args.length > 1 ? args[1] : "WORKER";
        int passos = args.length > 2 ? Integer.parseInt(args[2]) : 3;
        long pausaMs = args.length > 3 ? Long.parseLong(args[3]) : 250;
        int exitCode = args.length > 4 ? Integer.parseInt(args[4]) : 0;

        // Llegeix els noms transmesos per stdin si n'hi ha (com Ada, Pol, etc.)
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String linia;
            while (reader.ready() && (linia = reader.readLine()) != null) {
                log(nom, "Rebut per stdin: " + linia);
            }
        } catch (Exception ignored) {
        }

        for (int i = 1; i <= passos; i++) {
            log(nom, "pas " + i + "/" + passos);
            dormir(pausaMs);
        }

        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    // =========================================================================
    // 4. CAS TIMEOUT (Paso 3.1 & 3.2: Timeout, cancel·lació segura i ps)
    // =========================================================================
    public static void runTimeout() throws Exception {
        log("SUPERVISOR", "--- [DEMO 4: TIMEOUT I CANCEL·LACIÓ SEGURA] ---");
        // Llancem un procés que no acaba mai ("worker-hang") i li posem timeout de 2 segons
        List<String> cmd = javaCommand("worker-hang");
        log("SUPERVISOR", "Llançant worker que trigarà massa amb timeout de 2 segons...");

        ResultatExecucio r = executarProcesConcurrent(cmd, 2, TimeUnit.SECONDS);

        log("SUPERVISOR", "Resultat Timeout: PID=" + r.pid() + ", timedOut=" + r.timedOut() + ", codiRetorn=" + r.codiRetorn());
        if (r.timedOut()) {
            log("SUPERVISOR", "El procés s'ha cancel·lat correctament i no ha quedat orfe.");
        }
    }

    // =========================================================================
    // 5. CAS ERROR (Activitat d'anàlisi B: Diagnosi d'errors amb codi 7)
    // =========================================================================
    public static void runError() throws Exception {
        log("SUPERVISOR", "--- [DEMO 5: DIAGNOSI D'ERROR AMB CODI 7] ---");
        List<String> cmd = javaCommand("worker-error");
        ResultatExecucio r = executarProcesConcurrent(cmd, 5, TimeUnit.SECONDS);

        log("SUPERVISOR", "Resultat Worker fallit: PID=" + r.pid() + ", CodiRetorn=" + r.codiRetorn());
        if (r.codiRetorn() == 7) {
            log("SUPERVISOR", "L'error amb codi 7 ha quedat recollit i explícit amb èxit.");
        }
    }

    private static void execWorkerHang(String[] args) {
        String nom = args.length > 1 ? args[1] : "WORKER-GAMMA";
        log(nom, "Aquest procés simula quedar-se bloquejat.");
        dormir(30000); // 30 segons
    }

    private static void execWorkerError(String[] args) {
        String nom = args.length > 1 ? args[1] : "WORKER-BETA";
        log(nom, "Executant tasca que fallarà de manera controlada...");
        System.err.println("[ERROR CRÍTIC] S'ha produït una fallada interna simulada (Codi 7).");
        System.exit(7);
    }

    // =========================================================================
    // MÈTODES D'EXECUTOR I DRENATGE CONCURRENT (Pas 4 & 6)
    // =========================================================================
    public static ResultatExecucio executarProcesConcurrent(List<String> ordre, long timeout, TimeUnit unitat) throws Exception {
        return executarProcesConcurrent(ordre, null, timeout, unitat);
    }

    public static ResultatExecucio executarProcesConcurrent(List<String> ordre, String stdinPayload, long timeout, TimeUnit unitat) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(ordre);
        long tInici = System.currentTimeMillis();

        Process p = pb.start();
        long pid = p.pid();
        long ppid = ProcessHandle.current().pid();
        log("SUPERVISOR", "Procés fill iniciat amb PID: " + pid);

        // Si se li passa contingut per a stdin, l'escrivim i tanquem el flux (EOF)
        if (stdinPayload != null) {
            try (BufferedWriter writer = p.outputWriter(StandardCharsets.UTF_8)) {
                writer.write(stdinPayload);
                writer.flush();
            }
        }

        List<LiniaSortida> llistaSortides = Collections.synchronizedList(new ArrayList<>());

        Thread filStdout = new Thread(() -> drenarFlux(p.getInputStream(), CanalSortida.STDOUT, llistaSortides), "io-stdout");
        Thread filStderr = new Thread(() -> drenarFlux(p.getErrorStream(), CanalSortida.STDERR, llistaSortides), "io-stderr");

        filStdout.start();
        filStderr.start();

        boolean finalitzat = p.waitFor(timeout, unitat);
        boolean timedOut = false;
        int exitCode;

        if (!finalitzat) {
            timedOut = true;
            log("SUPERVISOR", "Timeout assolit (" + timeout + " " + unitat + ") pel PID: " + pid + ". Tancant...");
            p.destroy();
            if (!p.waitFor(1, TimeUnit.SECONDS)) {
                log("SUPERVISOR", "Forçant finalització amb destroyForcibly pel PID: " + pid);
                p.destroyForcibly();
            }
            exitCode = -1;
        } else {
            exitCode = p.exitValue();
        }

        filStdout.join();
        filStderr.join();

        long duradaMs = System.currentTimeMillis() - tInici;

        return new ResultatExecucio(
                List.copyOf(ordre),
                pid,
                ppid,
                exitCode,
                duradaMs,
                timedOut,
                List.copyOf(llistaSortides)
        );
    }

    private static void drenarFlux(InputStream is, CanalSortida canal, List<LiniaSortida> desti) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String linia;
            while ((linia = reader.readLine()) != null) {
                LiniaSortida ls = new LiniaSortida(Instant.now(), canal, linia);
                desti.add(ls);
            }
        } catch (Exception ignored) {
        }
    }

    private static void dormir(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}