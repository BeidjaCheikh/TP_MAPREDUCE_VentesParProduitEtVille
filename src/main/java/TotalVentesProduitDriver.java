import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalVentesProduitDriver {
    public static void main(String[] args) throws Exception {
        // Création d'une instance de Configuration qui contiendra les paramètres pour le job
        Configuration conf = new Configuration();

        // Création d'une instance de Job avec une description
        Job job = Job.getInstance(conf, "Prix Total des Ventes par Produit et Ville pour une Année Donnée");

        // Spécification de la classe principale (celle contenant la méthode main)
        job.setJarByClass(TotalVentesProduitDriver.class);

        // Spécification de la classe du Mapper à utiliser
        job.setMapperClass(VentesProduitMapper.class);

        // Spécification de la classe du Reducer à utiliser
        job.setReducerClass(VentesProduitReducer.class);

        // Spécification des types de clé et de valeur en sortie du Mapper
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Spécification des chemins des fichiers d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exécution du job et terminaison du programme
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
