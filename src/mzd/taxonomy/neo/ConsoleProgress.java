package mzd.taxonomy.neo;

import org.apache.commons.lang.StringUtils;

public class ConsoleProgress {
	private static String ACTION = "processed";
	private static int DEFAULT_STEPS = 10;
	private long max;
	private long tick = 0;
	private int steps = DEFAULT_STEPS;
	private int lastStep = 0;
	private int lastMinor = 0;
	private int stepSize;
	private int lastLineCh;
	private boolean first = true;
	private String bar = "";
	
	ConsoleProgress(int stepSize) {
		this.stepSize = stepSize;
	}
	
	ConsoleProgress(long max) {
		this.max = max;
		this.stepSize = (int)(max/steps);
		this.bar = String.format("[%s]", StringUtils.repeat(".", steps)); 
	}
	
	ConsoleProgress(long max, int steps) {
		this.max = max;
		this.steps = steps;
		this.stepSize = (int)(max/steps);
		this.bar = String.format("[%s]", StringUtils.repeat(".", steps)); 
	}
	
	
	void finish() {
		System.out.println();
	}
	
	void print() {
		if (++tick % stepSize == 0) {
			System.out.println(String.format("%d %s", tick, ACTION));
		}
	}
	
	void update() {
		++tick;
		printUpdate();
	}
	
	void updateTo(long tick) {
		this.tick = tick;
		printUpdate();
	}
	
	private void printUpdate() {
		if (first) {
			String msg = String.format("%s 0%% %s", bar, ACTION);
			lastLineCh = msg.length();
			System.out.print(msg);
			first = false;
		}
		else {
			int tickStep = (int)((double)tick / stepSize);
			// update progress bar if it has changed
			if (tickStep > lastStep) {
				bar = String.format("[%s%s]", 
						StringUtils.repeat("#", tickStep),
						StringUtils.repeat(".", steps - tickStep));
				lastStep = tickStep;
			}
			// print a tick update
			int minorStep = (int)((double)tick / (stepSize/10));
			if (minorStep > lastMinor) {
				String msg = String.format("%s %.1f%% %d %s",
						bar,
						100*(double)tick/max,
						tick,
						ACTION);
				String prefix = StringUtils.repeat("\b",lastLineCh);
				System.out.print(prefix + msg);
				lastLineCh = msg.length();
				lastMinor = minorStep;
			}
		}
	}

//	public static void main(String[] args) throws InterruptedException {
//		
//		ConsoleProgress p = new ConsoleProgress(1227000, 50);
//		synchronized (p) {
//			for (int i=0; i<1227000; i+=250) {
//				p.updateTo(i);
//				p.wait(50);
//			}
//		}
//		p.finish();
//	}
}