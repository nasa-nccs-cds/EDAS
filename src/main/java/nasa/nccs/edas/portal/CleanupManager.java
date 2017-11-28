package nasa.nccs.edas.portal;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import nasa.nccs.utilities.EDASLogManager;
import nasa.nccs.utilities.Logger;
import org.apache.commons.io.FileUtils;

public class CleanupManager {
    protected Logger logger = EDASLogManager.getCurrentLogger();

    public interface Executable {
        public void execute();
    }

    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    List<Executable> executables = new ArrayList<Executable>();
    volatile boolean isStopIssued;

    public CleanupManager( int targetHour, int targetMin, int targetSec ) {
        startExecutionAt( targetHour, targetMin, targetSec );
    }

    public void addExecutable( Executable ex ) { executables.add(ex); }

    public CleanupManager addFileCleanupTask( String directory, int lifetimeHrs, boolean removeDirectories, String fileFilter )  {
        FileCleanupTask task = new FileCleanupTask( directory, lifetimeHrs, removeDirectories, fileFilter );
        addExecutable( task );
        task.execute();
        return this;
    }

    public void runTasks() {
        for (int i = 0; i < executables.size(); i++) { executables.get(i).execute(); }
    }

    public void stop()  {
        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException ex) { ; }
    }

    private void startExecutionAt(int targetHour, int targetMin, int targetSec) {
        Runnable taskWrapper = new Runnable(){
            @Override
            public void run()  {
                runTasks();
                startExecutionAt(targetHour, targetMin, targetSec);
            }
        };
        long delay = computeNextDelay(targetHour, targetMin, targetSec);
        executorService.schedule(taskWrapper, delay, TimeUnit.SECONDS);
    }

    private long computeNextDelay(int targetHour, int targetMin, int targetSec)  {
        LocalDateTime localNow = LocalDateTime.now();
        ZoneId currentZone = ZoneId.systemDefault();
        ZonedDateTime zonedNow = ZonedDateTime.of(localNow, currentZone);
        ZonedDateTime zonedNextTarget = zonedNow.withHour(targetHour).withMinute(targetMin).withSecond(targetSec);
        if(zonedNow.compareTo(zonedNextTarget) > 0) zonedNextTarget = zonedNextTarget.plusDays(1);
        Duration duration = Duration.between(zonedNow, zonedNextTarget);
        return duration.getSeconds();
    }

    public class FilePermissionsTask implements Executable {
        String directory;
        String fileFilter = ".*";
        String perms = "rwxrwxrwx";

        public FilePermissionsTask( String directory$, String perms$,  String fileFilter$ ) {
            directory = directory$;
            fileFilter = fileFilter$;
            perms = perms$;
        }

        public void execute( ) {
            try {
                File folder = new File(directory);
                Files.setPosixFilePermissions(folder.toPath(), PosixFilePermissions.fromString(perms));
                File[] listOfFiles = folder.listFiles();
                for (int i = 0; i < listOfFiles.length; i++) {
                    File file = listOfFiles[i];
                    if (file.getName().matches(fileFilter)) {
                        Files.setPosixFilePermissions( file.toPath(), PosixFilePermissions.fromString(perms) );
                    }
                }
            } catch ( Exception ex ) {
                logger.error("Error setting perms in dir " + directory + ", error = " + ex.getMessage());
            }
        }
    }

    public class FileCleanupTask implements Executable {
        String directory;
        int lifetime = 24;
        String fileFilter = ".*";
        boolean removeDirectories = false;

        public FileCleanupTask( String directory$, int lifetime$, boolean removeDirectories$, String fileFilter$ ) {
            directory = directory$;
            lifetime = lifetime$;
            fileFilter = fileFilter$;
            removeDirectories = removeDirectories$;
        }
        public FileCleanupTask( String directory$, int lifetime$, boolean removeDirectories$ ) {
            directory = directory$;
            lifetime = lifetime$;
            removeDirectories = removeDirectories$;
        }
        public FileCleanupTask( String directory$, int lifetime$ ) {
            directory = directory$;
            lifetime = lifetime$;
        }
        public FileCleanupTask( String directory$ ) {
            directory = directory$;
        }
        public void execute( ) {
            File folder = new File( directory );
            File[] listOfFiles = folder.listFiles();
            for (int i = 0; i < listOfFiles.length; i++) {
                File file = listOfFiles[i];
                if( file.getName().matches(fileFilter) ) { cleanup( file ); }
            }
        }
        public void cleanup(File file) {
            long diff = new Date().getTime() - file.lastModified();
            if (diff > lifetime * 24 * 60 * 60 * 1000) {
                if (file.isFile() ) {
                    logger.info( "Cleaning up file " + file.getName() );
                    file.delete();
                }
                else if ( file.isDirectory() && removeDirectories ) {
                    try {
                        logger.info( "Cleaning up directory " + file.getName() );
                        FileUtils.deleteDirectory(file);
                    } catch ( Exception ex ) {
                        logger.error( "Error Cleaning up directory " + file.getName() + ", error = " + ex.getMessage() );
                    }
                }
            }
        }
    }
}
