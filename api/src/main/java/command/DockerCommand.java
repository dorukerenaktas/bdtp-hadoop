package command;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class DockerCommand {

    public static void RunCloseStdMapReduce(String input, String output) {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command(
                "docker", "exec", "namenode", "hadoop",
                "jar", "/opt/job/job-1.0-SNAPSHOT.jar", "CloseStdMapReduce", input, output);

        try {

            Process process = builder.start();

            StringBuilder result = new StringBuilder();

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line + "\n");
            }

            int exitVal = process.waitFor();
            if (exitVal == 0) {
                System.out.println("Success!");
                System.out.println(result);
                System.exit(0);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
