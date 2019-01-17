package com.bobo.flow;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author bobo
 * @Description:
 * @date 2019-01-02 14:09
 */
@Data
@ToString
@NoArgsConstructor
public class FlowBean implements Writable {

    private int upFlow;
    private int downFlow;
    private String phone;
    private int amountFlow;

    public FlowBean(int upFlow, int downFlow, String phone) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.phone = phone;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeUTF(phone);
        out.writeInt(amountFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readInt();
        downFlow = in.readInt();
        phone = in.readUTF();
        amountFlow = in.readInt();
    }
}
