import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class VentesProduitMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] valeurs = value.toString().split(" ");
        if (valeurs.length >= 4) {
            String ville = valeurs[1];
            String[] dateParts = valeurs[0].split("-");
            if (dateParts.length >= 1) {
                String annee = dateParts[0];
                try {
                    double prix = Double.parseDouble(valeurs[3]);
                    context.write(new Text(ville + "-" + annee), new DoubleWritable(prix));
                } catch (NumberFormatException e) {
                    //
                }
            }
        }
    }
}

