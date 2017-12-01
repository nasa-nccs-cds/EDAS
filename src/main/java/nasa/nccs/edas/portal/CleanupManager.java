package nasa.nccs.edas.portal;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Calendar;
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
    private static final long PERIOD = 24*60*60;
    private Date midnight = getMidnight();
    long initialDelay = (midnight.getTime()-System.currentTimeMillis())/1000;

    public CleanupManager() {
        startExecution();
    }
    Date getMidnight() {
        Calendar c = Calendar.getInstance();
        c.set( Calendar.HOUR_OF_DAY,23);
        c.set( Calendar.MINUTE,59);
        c.set( Calendar.SECOND,59);
        return c.getTime();
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

    private void startExecution() {
        Runnable taskWrapper = new Runnable(){
            @Override
            public void run()  { runTasks(); }
        };
        executorService.scheduleAtFixedRate(taskWrapper, initialDelay, PERIOD, TimeUnit.SECONDS);
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
        int lifetime = 48;
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
                if( file.getName().matches(fileFilter) ) {
                    cleanup( file );
                }
            }
        }
        public void cleanup(File file) {
            logger.info( " %C% ------------------------ Cleaning up directory ------------------------" + directory );
            long diff = new Date().getTime() - file.lastModified();
            if (diff > lifetime * 60 * 60 * 1000) {
                if (file.isFile() ) {
                    logger.info( " %C% ------ ------ ------> Cleaning up file " + file.getName() );
                    file.delete();
                }
                else if ( file.isDirectory() && removeDirectories ) {
                    try {
                        logger.info( " %C% ------ ------ ------> Cleaning up directory " + file.getName() );
                        FileUtils.deleteDirectory(file);
                    } catch ( Exception ex ) {
                        logger.error( "Error Cleaning up directory " + file.getName() + ", error = " + ex.getMessage() );
                    }
                }
            } else {
                logger.info( " %C% Retaining young file or dir: " + file.getName() );
            }
        }
    }
}
