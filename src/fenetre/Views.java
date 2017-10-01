package fenetre;

import enums.*;

import java.awt.BorderLayout;
import java.awt.Checkbox;
import java.awt.GridLayout;import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

public class Views extends JFrame{
	
	private Vector<Tohnoth> v;
	
	private JLabel nameHevraLabel;
	private JTextField nameHevraText;
	
	
	private JLabel cidLabel;
	private JTextField cidText;
	
	private JLabel pathFileLabel;
	private JTextField pathFileText;
	
	private JLabel year1Label;
	private JTextField year1Text;
	
	private JLabel year2Label;
	private JTextField year2Text;
	
	private JComboBox check;
	private JLabel label;
	
	private JButton alia;
	
	private JPanel pane;
	
	public Views(){
		
		this.v = new Vector<>();
		v.add(Tohnoth.NETO_MICPAL);
		v.add(Tohnoth.MICPAL);
		
		this.nameHevraText = new JTextField(10);
		this.cidText = new JTextField(10);
		this.pathFileText = new JTextField(10);
		
		this.year1Text = new JTextField(10);
		this.year2Text = new JTextField(10);
		
		this.nameHevraLabel = new JLabel("shem hevra : \n");
		this.cidLabel = new JLabel("CID : \n");
		this.pathFileLabel = new JLabel("Path : \n");
		
		this.year1Label = new JLabel("Year 1 : \n");
		this.year2Label = new JLabel("Year 2 : \n");
		this.label = new JLabel();
		
		this.alia = new Bouton("עליה ל101", null, null);
		
		this.check = new JComboBox<>(v);
		
		this.pane = new JPanel();
		
	    this.setLayout(new GridLayout(6, 2));
		
		this.add(nameHevraLabel);
		this.add(nameHevraText);
		
		this.add(cidLabel);
		this.add(cidText);
		
		this.add(pathFileLabel);
		this.add(pathFileText);
		
		this.add(nameHevraLabel);
		this.add(nameHevraText);
		
		this.add(year1Label);
		this.add(year1Text);
		
		this.add(year2Label);
		this.add(year2Text);
		this.add(check);
		
		this.add(alia);
		
		this.setVisible(true);
		this.setSize(300, 300);
		this.setLocationRelativeTo(null);
		this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	    
	}
	
	public static void main(String[] args){
		Views w = new Views();
	}
	
	public Vector<Tohnoth> getV() {
		return v;
	}

	public void setV(Vector<Tohnoth> v) {
		this.v = v;
	}

	public JLabel getNameHevraLabel() {
		return nameHevraLabel;
	}

	public void setNameHevraLabel(JLabel nameHevraLabel) {
		this.nameHevraLabel = nameHevraLabel;
	}

	public JTextField getNameHevraText() {
		return nameHevraText;
	}

	public void setNameHevraText(JTextField nameHevraText) {
		this.nameHevraText = nameHevraText;
	}

	public  JLabel getCidLabel() {
		return cidLabel;
	}

	public void setCidLabel(JLabel cidLabel) {
		this.cidLabel = cidLabel;
	}

	public JTextField getCidText() {
		return cidText;
	}

	public void setCidText(JTextField cidText) {
		this.cidText = cidText;
	}

	public JLabel getPathFileLabel() {
		return pathFileLabel;
	}

	public void setPathFileLabel(JLabel pathFileLabel) {
		this.pathFileLabel = pathFileLabel;
	}

	public JTextField getPathFileText() {
		return pathFileText;
	}

	public void setPathFileText(JTextField pathFileText) {
		this.pathFileText = pathFileText;
	}

	public JLabel getYear1Label() {
		return year1Label;
	}

	public void setYear1Label(JLabel year1Label) {
		this.year1Label = year1Label;
	}

	public JTextField getYear1Text() {
		return year1Text;
	}

	public void setYear1Text(JTextField year1Text) {
		this.year1Text = year1Text;
	}

	public JLabel getYear2Label() {
		return year2Label;
	}

	public void setYear2Label(JLabel year2Label) {
		this.year2Label = year2Label;
	}

	public JTextField getYear2Text() {
		return year2Text;
	}

	public void setYear2Text(JTextField year2Text) {
		this.year2Text = year2Text;
	}

	public JComboBox getCheck() {
		return check;
	}

	public void setCheck(JComboBox check) {
		this.check = check;
	}

	public JLabel getLabel() {
		return label;
	}

	public void setLabel(JLabel label) {
		this.label = label;
	}

	public JButton getAlia() {
		return alia;
	}

	public void setAlia(JButton alia) {
		this.alia = alia;
	}

	public JPanel getPane() {
		return pane;
	}

	public void setPane(JPanel pane) {
		this.pane = pane;
	}
}
