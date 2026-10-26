import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AEA1Test {

    private static int testsPassed = 0;
    private static int testsTotal = 0;

    private static void afegirTest(String nom, Runnable test) {
        testsTotal++;
        System.out.printf("Test [%d] %-40s ... ", testsTotal, nom);
        try {
            test.run();
            System.out.println("PASSED [OK]");
            testsPassed++;
        } catch (Throwable t) {
            System.out.println("FAILED [KO]: " + t.getMessage());
            t.printStackTrace(System.out);
        }
    }

    public static void main(String[] args) {
        System.out.println("=== BATERIA DE PROVES AUTOMATITZADES AEA1 ===");

        // Test 1: Execució correcta (Èxit)
        afegirTest("Execució amb èxit d'un worker", () -> {
            List<String> cmd = AEA1.javaCommand("worker", "WORKER-TEST");
            try {
                AEA1.ResultatExecucio r = AEA1.executarProcesConcurrent(cmd, 5, TimeUnit.SECONDS);
                if (r.codiRetorn() != 0) throw new AssertionError("S'esperava codi 0, obtingut: " + r.codiRetorn());
                if (r.timedOut()) throw new AssertionError("No s'esperava timeout");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Test 2: Ordre inexistent
        afegirTest("Llançament d'ordre inexistent", () -> {
            List<String> cmd = List.of("comanda_inexistent_12345");
            try {
                AEA1.executarProcesConcurrent(cmd, 2, TimeUnit.SECONDS);
                throw new AssertionError("Hauria d'haver llançat una IOException");
            } catch (java.io.IOException e) {
                // Comportament esperat
            } catch (Exception e) {
                throw new RuntimeException("S'esperava IOException, rebut: " + e);
            }
        });

        // Test 3: Gestió d'errors i codi de retorn explícit (Codi 7)
        afegirTest("Diagnosi d'error controlat (Codi 7)", () -> {
            List<String> cmd = AEA1.javaCommand("worker-error");
            try {
                AEA1.ResultatExecucio r = AEA1.executarProcesConcurrent(cmd, 5, TimeUnit.SECONDS);
                if (r.codiRetorn() != 7) throw new AssertionError("S'esperava codi 7, obtingut: " + r.codiRetorn());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Test 4: Timeout i cancel·lació forçada
        afegirTest("Timeout i finalització forçada", () -> {
            List<String> cmd = AEA1.javaCommand("worker-hang");
            try {
                AEA1.ResultatExecucio r = AEA1.executarProcesConcurrent(cmd, 1, TimeUnit.SECONDS);
                if (!r.timedOut()) throw new AssertionError("S'esperava que timedOut fos true");
                if (r.codiRetorn() != -1) throw new AssertionError("S'esperava codi -1 per timeout");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Test 5: Prova de la pipe Productor-Consumidor
        afegirTest("Pipe Productor cap a Consumidor (IPC)", () -> {
            try {
                AEA1.runPipe();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Test 6: Prova de concurrència i límit de 2
        afegirTest("Paral·lelisme amb pool de 2 workers", () -> {
            try {
                AEA1.runParalelisme();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        System.out.printf("%nResultat: %d / %d proves superades amb èxit!%n", testsPassed, testsTotal);
        if (testsPassed != testsTotal) {
            System.exit(1);
        }
    }
}
