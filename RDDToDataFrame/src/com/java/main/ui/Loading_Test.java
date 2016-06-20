package com.java.main.ui;

import java.awt.AlphaComposite;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Composite;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.beans.PropertyChangeEvent;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JLayer;
import javax.swing.JPanel;
import javax.swing.Timer;
import javax.swing.plaf.LayerUI;

/**
 * 
 * @author kbaghel Description - This class is used to show progress bar along
 *         with time elapsed
 */
public class Loading_Test {

	static final WaitLayerUI layerUI = new WaitLayerUI();

	JFrame frame = new JFrame("JLayer With Animated Gif");
	private JFrame mainFrame;
	private JPanel controlPanel, timePanel;
	private JLabel headerLabel;
	private JLabel time;

	long startTime = System.currentTimeMillis();
	String operation;

	public Loading_Test(String operationType) {
		
		operation = operationType;

		mainFrame = new JFrame("Data Quality Check");
		mainFrame.setSize(1920, 1030);
		mainFrame.setLocation(0, 0);
		mainFrame.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent windowEvent) {
				System.exit(0);
			}
		});

		headerLabel = new JLabel("", JLabel.CENTER);
		headerLabel.setText("<HTML><H2>Please wait....<H2><HTML>");

		time = new JLabel("", JLabel.CENTER);
		time.setFont(new Font("SansSerif", Font.BOLD, 25));
		time.setForeground(Color.black);

		JLabel background = new JLabel(new ImageIcon(Path.getImagePath()));
		background.setLayout(new BoxLayout(background, BoxLayout.PAGE_AXIS));

		timePanel = new JPanel();
		timePanel.setLayout(new FlowLayout());
		timePanel.setBackground(Color.WHITE);

		controlPanel = new JPanel();
		controlPanel.setLayout(new FlowLayout());
		controlPanel.setBackground(Color.WHITE);
		
		mainFrame.add(background);
		
		background.add(Box.createRigidArea(new Dimension(0, 200)));
		timePanel.add(time);
		background.add(timePanel);
		background.add(controlPanel);
		background.add(Box.createRigidArea(new Dimension(0, 300)));

		JPanel panel = new JPanel() {

			@Override
			public Dimension getPreferredSize() {
				return new Dimension(200, 200);
			}
		};
		panel.setBackground(Color.decode("#0C7AA2"));
		panel.add(Box.createRigidArea(new Dimension(0, 180)));
		panel.add(headerLabel);

		JLayer<JPanel> jlayer = new JLayer<>(panel, layerUI);
		jlayer.setBackground(Color.WHITE);
		controlPanel.add(jlayer);

		mainFrame.setVisible(true);
		layerUI.start();

		// starting new Thread which will update time
		new Thread(new Runnable() {
			public void run() {
				try {
					updateTime();
				} catch (Exception ie) {
				}
			}
		}).start();

	}

	public void stopProgressBas() {
		mainFrame.setVisible(false);
	}

	/*
	 * public void runWaitBar() {
	 * 
	 * java.awt.EventQueue.invokeLater(new Runnable() {
	 * 
	 * @Override public void run() { Loading_Test loading_Test = new
	 * Loading_Test();
	 * 
	 * 
	 * } }); }
	 */

	public void updateTime() {
		try {
			while (true) {
				// geting Time in desire format
				time.setText(operation + ". Time Elapsed - " + getTimeElapsed());
				// Thread sleeping for 1 sec
				Thread.currentThread().sleep(1000);
			}
		} catch (Exception e) {
			System.out.println("Exception in Thread Sleep : " + e);
		}
	}

	public String getTimeElapsed() {
		long elapsedTime = System.currentTimeMillis() - startTime;
		elapsedTime = elapsedTime / 1000;

		String seconds = Integer.toString((int) (elapsedTime % 60));
		String minutes = Integer.toString((int) ((elapsedTime % 3600) / 60));
		String hours = Integer.toString((int) (elapsedTime / 3600));

		if (seconds.length() < 2)
			seconds = "0" + seconds;

		if (minutes.length() < 2)
			minutes = "0" + minutes;

		if (hours.length() < 2)
			hours = "0" + hours;

		return hours + ":" + minutes + ":" + seconds;
	}
}

class WaitLayerUI extends LayerUI<JPanel> implements ActionListener {

	private boolean mIsRunning;
	private boolean mIsFadingOut;
	private Timer mTimer;
	private int mAngle;
	private int mFadeCount;
	private int mFadeLimit = 15;

	@Override
	public void paint(Graphics g, JComponent c) {
		int w = c.getWidth();
		int h = c.getHeight();
		super.paint(g, c); // Paint the view.
		if (!mIsRunning) {
			return;
		}
		Graphics2D g2 = (Graphics2D) g.create();
		float fade = (float) mFadeCount / (float) mFadeLimit;
		Composite urComposite = g2.getComposite(); // Gray it out.
		g2.setComposite(AlphaComposite.getInstance(AlphaComposite.SRC_OVER,
				.5f * fade));
		g2.fillRect(0, 0, w, h);
		g2.setComposite(urComposite);
		int s = Math.min(w, h) / 5;// Paint the wait indicator.
		int cx = w / 2;
		int cy = h / 2;
		g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
				RenderingHints.VALUE_ANTIALIAS_ON);
		g2.setStroke(new BasicStroke(s / 4, BasicStroke.CAP_ROUND,
				BasicStroke.JOIN_ROUND));
		g2.setPaint(Color.white);
		g2.rotate(Math.PI * mAngle / 180, cx, cy);
		for (int i = 0; i < 12; i++) {
			float scale = (11.0f - (float) i) / 11.0f;
			g2.drawLine(cx + s, cy, cx + s * 2, cy);
			g2.rotate(-Math.PI / 6, cx, cy);
			g2.setComposite(AlphaComposite.getInstance(AlphaComposite.SRC_OVER,
					scale * fade));
		}
		g2.dispose();
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		if (mIsRunning) {

			firePropertyChange("tick", 0, 1);
			mAngle += 3;
			if (mAngle >= 360) {
				mAngle = 0;
			}
			if (mIsFadingOut) {
				if (--mFadeCount == 0) {
					mIsRunning = false;
					mTimer.stop();
				}
			} else if (mFadeCount < mFadeLimit) {
				mFadeCount++;
			}
		}
	}

	public void start() {
		if (mIsRunning) {
			return;
		}
		mIsRunning = true;// Run a thread for animation.
		mIsFadingOut = false;
		mFadeCount = 0;
		int fps = 24;
		int tick = 1000 / fps;
		mTimer = new Timer(tick, this);
		mTimer.start();
	}

	public void stop() {
		mIsFadingOut = true;
	}

	@Override
	public void applyPropertyChange(PropertyChangeEvent pce, JLayer l) {
		if ("tick".equals(pce.getPropertyName())) {
			l.repaint();
		}
	}
}
