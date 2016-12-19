package lstock.ui;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.Font;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.DefaultListModel;
import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Alignment;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JToggleButton;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.UIManager;
import javax.swing.border.EmptyBorder;
import javax.swing.border.EtchedBorder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lstock.AnnualCalc;
import lstock.AnnualImportDividend;
import lstock.DailyImportTrade;
import lstock.MonthlyImportRevenue;
import lstock.MonthlyImportTaiExPrice;
import lstock.MyDB;
import lstock.QuarterlyCalc;
import lstock.QuarterlyFixCashflow;
import lstock.QuarterlyFixQ4Income;
import lstock.QuarterlyImport;
import lstock.QuarterlyImportOldCashflow;

public class RebuildUI extends JFrame {
	private static final Logger log = LogManager.getLogger(lstock.ui.RebuildUI.class.getName());
	/**
	 * 
	 */
	private static final long serialVersionUID = -3457352080844669200L;

	private JPanel contentPane;
	
	private static ImageIcon iconCheck;
	private static ImageIcon iconSync;
	private static ImageIcon iconNull;
	private static ImageIcon iconSwitchOn;
	private static ImageIcon iconSwitchOff;
	private static ImageIcon iconIgnore;
	
	private static JTextArea txtDownload;
	private static DefaultListModel<String> importListModel;
	private static JList<String> listProcess;
	private static JProgressBar progressBar;
	private static JLabel lbDailyTrade;
	private static JLabel lbDailyTaiEx;
	private static JLabel lbMonthlyRevenue;
	private static JLabel lbQuarterlyTable;
	private static JLabel lbQuarterlyFixQ4Incom;
	private static JLabel lbQuarterlyFixCashflow;
	private static JLabel lbOldCashflow;
	private static JLabel lbQuarterlyCalc;
	private static JLabel lbAnnualCalc;
	private static JLabel lbDividend;
	private static JToggleButton tglDividend;

	
	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
				
		try {
			// Set System L&F
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (Exception e) {
			// handle exception
		}

		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					RebuildUI frame = new RebuildUI();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	void resetUI() {
		lbDailyTrade.setIcon(iconNull);
		lbDailyTaiEx.setIcon(iconNull);
		lbMonthlyRevenue.setIcon(iconNull);
		lbQuarterlyTable.setIcon(iconNull);
		lbQuarterlyFixQ4Incom.setIcon(iconNull);
		lbQuarterlyFixCashflow.setIcon(iconNull);
		lbOldCashflow.setIcon(iconNull);
		lbQuarterlyCalc.setIcon(iconNull);
		lbAnnualCalc.setIcon(iconNull);
		lbDividend.setIcon(iconNull);
		
		txtDownload.setText(null);
		progressBar.setString(null);
	}
	
	void runRebuild() {
		MyDB db;
		try {
			
			resetUI();
		
			db = new MyDB();
			log.info("Import Daily Trade");
			
			lbDailyTrade.setIcon(iconSync);
			DailyImportTrade.importToDB(db);
			lbDailyTrade.setIcon(iconCheck);

			log.info("Import Daily TaiEx Price");
			
			lbDailyTaiEx.setIcon(iconSync);
			MonthlyImportTaiExPrice.importToDB(db);
			lbDailyTaiEx.setIcon(iconCheck);

			log.info("Import Monthly Revenue");
			
			lbMonthlyRevenue.setIcon(iconSync);
			MonthlyImportRevenue.importToDB(db);
			lbMonthlyRevenue.setIcon(iconCheck);

			log.info("Import Quarterly Tables");
			
			lbQuarterlyTable.setIcon(iconSync);
			QuarterlyImport.importToDB(db);
			lbQuarterlyTable.setIcon(iconCheck);
			
			lbQuarterlyFixQ4Incom.setIcon(iconSync);
			QuarterlyFixQ4Income.fixIncome(db);
			lbQuarterlyFixQ4Incom.setIcon(iconCheck);
			
			lbQuarterlyFixCashflow.setIcon(iconSync);
			QuarterlyFixCashflow.fixCashflow(db);
			lbQuarterlyFixCashflow.setIcon(iconCheck);
			
			lbOldCashflow.setIcon(iconSync);
			QuarterlyImportOldCashflow.importOldCashflowToDb(db);
			lbOldCashflow.setIcon(iconCheck);
			
			lbQuarterlyCalc.setIcon(iconSync);
			QuarterlyCalc.calculateAllCompanies(db);
			lbQuarterlyCalc.setIcon(iconCheck);
			
			lbAnnualCalc.setIcon(iconSync);
			AnnualCalc.calculateAllCompanies(db);
			lbAnnualCalc.setIcon(iconCheck);

			if (tglDividend.isSelected()) {
				log.info("Import Annual Dividend");
				lbDividend.setIcon(iconSync);
				AnnualImportDividend.importToDB(db);
				lbDividend.setIcon(iconCheck);
			} else {
				lbDividend.setIcon(iconIgnore);
			}
			

			db.close();
			log.info("Done !!");
			
			progressBar.setString("Completed");
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * Create the frame.
	 */
	public RebuildUI() {
		setMinimumSize(new Dimension(800, 600));
		iconNull = new ImageIcon(RebuildUI.class.getResource("/image/null_24px.png"));
		iconCheck = new ImageIcon(RebuildUI.class.getResource("/image/Ok_24px.png"));
		iconSync =  new ImageIcon(RebuildUI.class.getResource("/image/Synchronize_24px.png"));
		iconSwitchOn = new ImageIcon(RebuildUI.class.getResource("/image/Switch On_16px.png"));
		iconSwitchOff = new ImageIcon(RebuildUI.class.getResource("/image/Switch Off_16px.png"));
		iconIgnore = new ImageIcon(RebuildUI.class.getResource("/image/Silent_24px.png"));
		
		setTitle("匯入全部資料");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 800, 600);
		setLocationRelativeTo(null);
		contentPane = new JPanel();
		contentPane.setBorder(null);
		setContentPane(contentPane);

		JSplitPane splitPane = new JSplitPane();
		splitPane.setBorder(new EmptyBorder(0, 0, 0, 0));
		splitPane.setEnabled(false);
		splitPane.setDividerSize(1);

		JPanel panelRight = new JPanel();
		splitPane.setRightComponent(panelRight);

		JButton btnRebuild = new JButton("Rebuild");
		btnRebuild.setAlignmentY(Component.TOP_ALIGNMENT);
		btnRebuild.setBorder(UIManager.getBorder("Button.border"));

		importListModel = new DefaultListModel<String>();

		JScrollPane scrollPaneImport = new JScrollPane();
		scrollPaneImport.setFocusable(false);
		scrollPaneImport.setFocusTraversalKeysEnabled(false);
		scrollPaneImport.setAutoscrolls(true);
		listProcess = new JList<String>(importListModel);
		listProcess.setFocusTraversalKeysEnabled(false);
		listProcess.setFocusable(false);
		scrollPaneImport.setViewportView(listProcess);
		listProcess.setBorder(new EtchedBorder(EtchedBorder.LOWERED, null, null));

		JLabel lbProcess = new JLabel("處理中執行緒：");

		JLabel lbDownload = new JLabel("下載：");

		JScrollPane scrollPaneDownload = new JScrollPane();
		scrollPaneDownload.setFocusTraversalKeysEnabled(false);
		GroupLayout gl_panelRight = new GroupLayout(panelRight);
		gl_panelRight.setHorizontalGroup(
			gl_panelRight.createParallelGroup(Alignment.TRAILING)
				.addGroup(gl_panelRight.createSequentialGroup()
					.addGroup(gl_panelRight.createParallelGroup(Alignment.LEADING)
						.addGroup(gl_panelRight.createSequentialGroup()
							.addGap(20)
							.addComponent(scrollPaneDownload, GroupLayout.DEFAULT_SIZE, 343, Short.MAX_VALUE))
						.addGroup(gl_panelRight.createSequentialGroup()
							.addGap(20)
							.addComponent(scrollPaneImport, GroupLayout.DEFAULT_SIZE, 343, Short.MAX_VALUE))
						.addGroup(Alignment.TRAILING, gl_panelRight.createSequentialGroup()
							.addContainerGap(316, Short.MAX_VALUE)
							.addComponent(btnRebuild, GroupLayout.PREFERRED_SIZE, 87, GroupLayout.PREFERRED_SIZE))
						.addGroup(gl_panelRight.createSequentialGroup()
							.addContainerGap()
							.addComponent(lbProcess, GroupLayout.PREFERRED_SIZE, 125, GroupLayout.PREFERRED_SIZE))
						.addGroup(gl_panelRight.createSequentialGroup()
							.addContainerGap()
							.addComponent(lbDownload, GroupLayout.PREFERRED_SIZE, 125, GroupLayout.PREFERRED_SIZE)))
					.addContainerGap())
		);
		gl_panelRight.setVerticalGroup(
			gl_panelRight.createParallelGroup(Alignment.TRAILING)
				.addGroup(gl_panelRight.createSequentialGroup()
					.addGap(10)
					.addComponent(lbDownload, GroupLayout.PREFERRED_SIZE, 15, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(scrollPaneDownload, GroupLayout.PREFERRED_SIZE, 100, GroupLayout.PREFERRED_SIZE)
					.addGap(18)
					.addComponent(lbProcess)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(scrollPaneImport, GroupLayout.DEFAULT_SIZE, 199, Short.MAX_VALUE)
					.addGap(10)
					.addComponent(btnRebuild)
					.addContainerGap())
		);
		
		txtDownload = new JTextArea();
		txtDownload.setFocusable(false);
		txtDownload.setFocusTraversalKeysEnabled(false);
		scrollPaneDownload.setViewportView(txtDownload);
		panelRight.setLayout(gl_panelRight);
		btnRebuild.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {

				Thread thread = new Thread(new Runnable() {
					public void run() {
						runRebuild();
					}
				});
				thread.setDaemon(true);
				thread.start();
			}
		});

		JPanel panelLeft = new JPanel();
		panelLeft.setBackground(new Color(100, 149, 237));
		splitPane.setLeftComponent(panelLeft);

		lbDailyTrade = new JLabel("匯入每日個股股價");
		lbDailyTrade.setFont(new Font("微軟正黑體", Font.BOLD, 14));
		lbDailyTrade.setMinimumSize(new Dimension(0, 24));
		lbDailyTrade.setPreferredSize(new Dimension(0, 24));
		lbDailyTrade.setIcon(iconNull);
		lbDailyTrade.setForeground(Color.WHITE);

		lbDailyTaiEx = new JLabel("匯入每日加權指數");
		lbDailyTaiEx.setFont(new Font("微軟正黑體", Font.BOLD, 14));
		lbDailyTaiEx.setMinimumSize(new Dimension(0, 24));
		lbDailyTaiEx.setPreferredSize(new Dimension(0, 24));
		lbDailyTaiEx.setIcon(iconNull);
		lbDailyTaiEx.setForeground(Color.WHITE);

		lbMonthlyRevenue = new JLabel("匯入每月營收");
		lbMonthlyRevenue.setFont(new Font("微軟正黑體", Font.BOLD, 14));
		lbMonthlyRevenue.setMinimumSize(new Dimension(0, 24));
		lbMonthlyRevenue.setPreferredSize(new Dimension(0, 24));
		lbMonthlyRevenue.setIcon(iconNull);
		lbMonthlyRevenue.setForeground(Color.WHITE);

		lbQuarterlyTable = new JLabel("匯入季報");
		lbQuarterlyTable.setFont(new Font("微軟正黑體", Font.BOLD, 14));
		lbQuarterlyTable.setMinimumSize(new Dimension(0, 24));
		lbQuarterlyTable.setPreferredSize(new Dimension(0, 24));
		lbQuarterlyTable.setIcon(iconNull);
		lbQuarterlyTable.setForeground(Color.WHITE);
		
		lbQuarterlyFixQ4Incom = new JLabel("修正第四季損益表");
		lbQuarterlyFixQ4Incom.setFont(new Font("微軟正黑體", Font.BOLD, 14));
		lbQuarterlyFixQ4Incom.setMinimumSize(new Dimension(0, 24));
		lbQuarterlyFixQ4Incom.setPreferredSize(new Dimension(0, 24));
		lbQuarterlyFixQ4Incom.setIcon(iconNull);
		lbQuarterlyFixQ4Incom.setForeground(Color.WHITE);
		
		lbQuarterlyFixCashflow = new JLabel("修正現金流量表");
		lbQuarterlyFixCashflow.setFont(new Font("微軟正黑體", Font.BOLD, 14));
		lbQuarterlyFixCashflow.setMinimumSize(new Dimension(0, 24));
		lbQuarterlyFixCashflow.setPreferredSize(new Dimension(0, 24));
		lbQuarterlyFixCashflow.setIcon(iconNull);
		lbQuarterlyFixCashflow.setForeground(Color.WHITE);
		
		lbOldCashflow = new JLabel("匯入2013前現金流量表");
		lbOldCashflow.setFont(new Font("微軟正黑體", Font.BOLD, 14));
		lbOldCashflow.setMinimumSize(new Dimension(0, 24));
		lbOldCashflow.setPreferredSize(new Dimension(0, 24));
		lbOldCashflow.setIcon(iconNull);
		lbOldCashflow.setForeground(Color.WHITE);
		
		lbQuarterlyCalc = new JLabel("計算季報其餘欄位");
		lbQuarterlyCalc.setFont(new Font("微軟正黑體", Font.BOLD, 14));
		lbQuarterlyCalc.setMinimumSize(new Dimension(0, 24));
		lbQuarterlyCalc.setPreferredSize(new Dimension(0, 24));
		lbQuarterlyCalc.setIcon(iconNull);
		lbQuarterlyCalc.setForeground(Color.WHITE);

		lbAnnualCalc = new JLabel("計算年報其餘欄位");
		lbAnnualCalc.setFont(new Font("微軟正黑體", Font.BOLD, 14));
		lbAnnualCalc.setMinimumSize(new Dimension(0, 24));
		lbAnnualCalc.setPreferredSize(new Dimension(0, 24));
		lbAnnualCalc.setIcon(iconNull);
		lbAnnualCalc.setForeground(Color.WHITE);

		lbDividend = new JLabel("匯入股利資料");
		lbDividend.setFont(new Font("微軟正黑體", Font.BOLD, 14));
		lbDividend.setMinimumSize(new Dimension(0, 24));
		lbDividend.setPreferredSize(new Dimension(0, 24));
		lbDividend.setIcon(iconNull);
		lbDividend.setForeground(Color.WHITE);


		progressBar = new JProgressBar();
		progressBar.setAlignmentX(0.0f);
		progressBar.setForeground(new Color(30, 144, 255));
		progressBar.setAlignmentY(0.0f);
		progressBar.setStringPainted(true);
		
		tglDividend = new JToggleButton("");
		tglDividend.setAlignmentY(Component.TOP_ALIGNMENT);
		tglDividend.setFocusable(false);
		tglDividend.setToolTipText("是否執行股利匯入");
		tglDividend.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (!tglDividend.isSelected()) {
					tglDividend.setIcon(iconSwitchOff);
				} else {
					tglDividend.setIcon(iconSwitchOn);
				}
			}
		});
		
		

		tglDividend.setBorder(null);
		tglDividend.setFocusPainted(false);
		tglDividend.setMargin(new Insets(0, 0, 0, 0));
		tglDividend.setIcon(iconSwitchOff);
		
		GroupLayout gl_panelLeft = new GroupLayout(panelLeft);
		gl_panelLeft.setHorizontalGroup(
			gl_panelLeft.createParallelGroup(Alignment.LEADING)
				.addComponent(progressBar, GroupLayout.DEFAULT_SIZE, 250, Short.MAX_VALUE)
				.addGroup(gl_panelLeft.createSequentialGroup()
					.addContainerGap()
					.addGroup(gl_panelLeft.createParallelGroup(Alignment.LEADING)
						.addComponent(lbDailyTrade, GroupLayout.PREFERRED_SIZE, 180, GroupLayout.PREFERRED_SIZE)
						.addComponent(lbDailyTaiEx, GroupLayout.PREFERRED_SIZE, 180, GroupLayout.PREFERRED_SIZE)
						.addComponent(lbMonthlyRevenue, GroupLayout.PREFERRED_SIZE, 180, GroupLayout.PREFERRED_SIZE)
						.addComponent(lbQuarterlyTable, GroupLayout.PREFERRED_SIZE, 180, GroupLayout.PREFERRED_SIZE)
						.addComponent(lbQuarterlyFixQ4Incom, GroupLayout.PREFERRED_SIZE, 180, GroupLayout.PREFERRED_SIZE)
						.addComponent(lbQuarterlyFixCashflow, GroupLayout.PREFERRED_SIZE, 180, GroupLayout.PREFERRED_SIZE)
						.addComponent(lbOldCashflow, GroupLayout.PREFERRED_SIZE, 180, GroupLayout.PREFERRED_SIZE)
						.addComponent(lbQuarterlyCalc, GroupLayout.PREFERRED_SIZE, 180, GroupLayout.PREFERRED_SIZE)
						.addComponent(lbAnnualCalc, GroupLayout.PREFERRED_SIZE, 180, GroupLayout.PREFERRED_SIZE)
						.addGroup(gl_panelLeft.createSequentialGroup()
							.addComponent(lbDividend, GroupLayout.PREFERRED_SIZE, 180, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.RELATED, 24, Short.MAX_VALUE)
							.addComponent(tglDividend)))
					.addContainerGap())
		);
		gl_panelLeft.setVerticalGroup(
			gl_panelLeft.createParallelGroup(Alignment.TRAILING)
				.addGroup(gl_panelLeft.createSequentialGroup()
					.addGap(28)
					.addGroup(gl_panelLeft.createParallelGroup(Alignment.TRAILING)
						.addGroup(gl_panelLeft.createSequentialGroup()
							.addComponent(lbDailyTrade, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(lbDailyTaiEx, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(lbMonthlyRevenue, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(lbQuarterlyTable, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(lbQuarterlyFixQ4Incom, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(lbQuarterlyFixCashflow, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(lbOldCashflow, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(lbQuarterlyCalc, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(lbAnnualCalc, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(lbDividend, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
						.addComponent(tglDividend))
					.addPreferredGap(ComponentPlacement.RELATED, 216, Short.MAX_VALUE)
					.addComponent(progressBar, GroupLayout.PREFERRED_SIZE, 24, GroupLayout.PREFERRED_SIZE))
		);
		panelLeft.setLayout(gl_panelLeft);
		splitPane.setDividerLocation(250);
		GroupLayout gl_contentPane = new GroupLayout(contentPane);
		gl_contentPane.setHorizontalGroup(gl_contentPane.createParallelGroup(Alignment.LEADING).addComponent(splitPane,
				Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, 574, Short.MAX_VALUE));
		gl_contentPane.setVerticalGroup(gl_contentPane.createParallelGroup(Alignment.LEADING).addComponent(splitPane,
				GroupLayout.DEFAULT_SIZE, 329, Short.MAX_VALUE));
		contentPane.setLayout(gl_contentPane);
	}
	
	public static void addDownload(final String str) {
		if (txtDownload == null)
			return;

		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			@Override
			public void run() {
				txtDownload.append(str + "\n");
			}
		});
	}

	public static void addProcess(final String str) {
		if (importListModel == null)
			return;

		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			@Override
			public void run() {
				importListModel.addElement(str);
			}
		});
	}

	public static void removeProcess(final String str) {
		if (importListModel == null)
			return;

		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			@Override
			public void run() {
				importListModel.removeElement(str);
			}
		});
	}

	public static void updateProgressBar(final int percentage) {
		if (progressBar == null)
			return;

		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			@Override
			public void run() {
				progressBar.setValue(percentage);
			}
		});
	}
}
