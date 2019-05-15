package com.sankuai.inf.leaf.server;

import com.sankuai.inf.leaf.segment.SegmentIDGenImpl;
import com.sankuai.inf.leaf.server.model.SegmentBufferView;
import com.sankuai.inf.leaf.segment.model.LeafAlloc;
import com.sankuai.inf.leaf.segment.model.SegmentBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 号段模式监控Controller
 */
@Controller
public class LeafMonitorController {
    private Logger logger = LoggerFactory.getLogger(LeafMonitorController.class);

    @Autowired
    SegmentService segmentService;

    @RequestMapping(value = "cache")
    public String getCache(Model model) {
        Map<String, SegmentBufferView> data = new HashMap<>();

        // 获取号段模式id生成器
        SegmentIDGenImpl segmentIDGen = segmentService.getIdGen();
        if (segmentIDGen == null) {
            throw new IllegalArgumentException("You should config leaf.segment.enable=true first");
        }
        // 获取cache，遍历cache的每个key，将SegmentBuffer中的双buffer信息封装为SegmentBufferView
        Map<String, SegmentBuffer> cache = segmentIDGen.getCache();
        for (Map.Entry<String, SegmentBuffer> entry : cache.entrySet()) {
            SegmentBufferView sv = new SegmentBufferView();
            SegmentBuffer buffer = entry.getValue();

            // buffer是否DB数据初始化
            sv.setInitOk(buffer.isInitOk());
            sv.setKey(buffer.getKey());
            // 当前正在使用的Segment
            sv.setPos(buffer.getCurrentPos());
            // 另一个Segment是否异步准备好
            sv.setNextReady(buffer.isNextReady());
            // 0号Segment
            sv.setMax0(buffer.getSegments()[0].getMax());
            sv.setValue0(buffer.getSegments()[0].getValue().get());
            sv.setStep0(buffer.getSegments()[0].getStep());
            // 1号Segment
            sv.setMax1(buffer.getSegments()[1].getMax());
            sv.setValue1(buffer.getSegments()[1].getValue().get());
            sv.setStep1(buffer.getSegments()[1].getStep());

            data.put(entry.getKey(), sv);

        }
        logger.info("Cache info {}", data);
        model.addAttribute("data", data);
        return "segment";
    }

    /**
     * 查看db数据库中所有的记录信息
     * @param model
     * @return
     */
    @RequestMapping(value = "db")
    public String getDb(Model model) {
        SegmentIDGenImpl segmentIDGen = segmentService.getIdGen();
        if (segmentIDGen == null) {
            throw new IllegalArgumentException("You should config leaf.segment.enable=true first");
        }
        List<LeafAlloc> items = segmentIDGen.getAllLeafAllocs();
        logger.info("DB info {}", items);
        model.addAttribute("items", items);
        return "db";
    }

}
