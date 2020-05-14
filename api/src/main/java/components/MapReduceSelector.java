package components;

import javax.swing.*;

public class MapReduceSelector extends JComboBox<String> {

    private static final String[] mapReduceJobs = new String[] {
            "Close Price Standard Deviation Map-Reduce",
            "Max Price Difference Map-Reduce",
            "Min Open Close Difference Map-Reduce",
            "Min Open Price Map-Reduce",
            "Volume Average Map-Reduce"
    };

    public MapReduceSelector() {
        super(mapReduceJobs);

        this.setVisible(true);
        this.addActionListener(this);
    }
}
