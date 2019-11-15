package mapreduce.fraudDetection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


//Data Explanation:
/*
Col1: customerId
col2: customer name
col3 : date on which customer placed order
col4 : shipping date
col5: shipping courier
col6: date on which product was received by the customer
col7: yes if customer has returned the product otherwise no
col8: return date when product was returned
col9: reason for return

Fraud Points:
Assign 1 fraud point to a transaction/customer whose (refund_date - receiving_date) > 10 days
Assign 10 fraud points  to the customer whose return date is more than or equal to 50% where return rate = (no. of orders returned) / (no. of orders by a customer) * 100

Output as decreasing order of fraud points . Highest fraud customer should come at top and least fraud customer should come at last.
 */
public class FraudWritable implements Writable
{
//We are going to output only values not keys so there is no need to compare anything thus no need of writableComparable
    private String customerName;
    private String receiveDate;
    private boolean returned;
    private String returnDate;

    public FraudWritable()
    {
        set("", "", "no", "");
    }

    //will set value of four variable . Value is set from mapper class by using objectname.setmethod
    /* getters and setters for custom values */
    public void set(String customerName, String receiveDate, String returned, String returnDate)
    {
        this.customerName = customerName;
        this.receiveDate = receiveDate;
        if (returned.equalsIgnoreCase("yes"))
            this.returned = true;
        else
            this.returned = false;
        this.returnDate = returnDate;
    }

    public String getCustomerName()
    {
        return this.customerName;
    }
    public String getReceiveDate()
    {
        return this.receiveDate;
    }
    public boolean getReturned()
    {
        return this.returned;
    }
    public String getReturnDate()
    {
        return this.returnDate;
    }

    // Mandatory functions for Hadoop to know how to write values for this class

    //to convert data to bytestream. include all the variable here
    @Override
    public void write(DataOutput out) throws IOException
    {
        WritableUtils.writeString(out, this.customerName);
        WritableUtils.writeString(out, this.receiveDate);
        out.writeBoolean(this.returned);
        WritableUtils.writeString(out, this.returnDate);
    }
    // Mandatory functions for Hadoop to know how to read values for this class
    @Override
    public void readFields(DataInput in) throws IOException
    {
        this.customerName = WritableUtils.readString(in);
        this.receiveDate = WritableUtils.readString(in);
        this.returned = in.readBoolean();
        this.returnDate = WritableUtils.readString(in);
    }
}
