/*
 * *****************************************************************************
 * Copyright (C) 2014-2026 Dennis Sheirer
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 * ****************************************************************************
 */
package io.github.dsheirer.gui.preference.scanner;

import io.github.dsheirer.monitor.ScannerLink;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;

/**
 * Rod build: User Preferences > Application > Scanner - the scanner SDR-Trunk reports to (status and remote control
 * from the scanner's admin page). Separate from the streaming configurations.
 */
public class ScannerLinkPreferenceEditor extends HBox
{
    private final TextField mUrl = new TextField();
    private final PasswordField mKeyHidden = new PasswordField();
    private final TextField mKeyShown = new TextField();
    private final Label mStatus = new Label();

    public ScannerLinkPreferenceEditor()
    {
        setMaxWidth(Double.MAX_VALUE);
        GridPane grid = new GridPane();
        grid.setVgap(10);
        grid.setHgap(8);
        grid.setPadding(new Insets(10, 10, 10, 10));
        int row = 0;

        Label about = new Label("The scanner SDR-Trunk reports to, so the scanner's admin page can show and control it. " +
            "This is separate from the streaming configurations - no stream is needed for it.");
        about.setWrapText(true);
        grid.add(about, 0, row, 3, 1);

        mUrl.setText(ScannerLink.getUrl());
        mUrl.setPromptText("https://your-scanner-address");
        grid.add(new Label("Scanner URL"), 0, ++row);
        grid.add(mUrl, 1, row, 2, 1);

        mKeyHidden.setText(ScannerLink.getApiKey());
        mKeyShown.textProperty().bindBidirectional(mKeyHidden.textProperty());
        mKeyShown.setVisible(false);
        mKeyShown.setManaged(false);
        CheckBox show = new CheckBox("Show");
        show.selectedProperty().addListener((o, was, on) -> {
            mKeyShown.setVisible(on);
            mKeyShown.setManaged(on);
            mKeyHidden.setVisible(!on);
            mKeyHidden.setManaged(!on);
        });
        HBox keyBox = new HBox(6, mKeyHidden, mKeyShown);
        HBox.setHgrow(mKeyHidden, Priority.ALWAYS);
        HBox.setHgrow(mKeyShown, Priority.ALWAYS);
        grid.add(new Label("API Key"), 0, ++row);
        grid.add(keyBox, 1, row);
        grid.add(show, 2, row);

        Button save = new Button("Save");
        save.setOnAction(e -> {
            ScannerLink.set(mUrl.getText(), mKeyHidden.getText());
            mUrl.setText(ScannerLink.getUrl());
            mStatus.setText(ScannerLink.isUsable() ? "Saved - the next status report goes to this scanner."
                : "Saved - reports are off until both the Scanner URL and API Key are filled in.");
        });
        grid.add(save, 1, ++row);
        grid.add(mStatus, 1, ++row, 2, 1);
        mStatus.setWrapText(true);

        Label hint = new Label("Use the scanner's address only (for example https://scanner.example.com) and an API key " +
            "from the scanner's admin page. Saved right away - no restart needed.");
        hint.setWrapText(true);
        grid.add(hint, 0, ++row, 3, 1);

        ColumnConstraints c1 = new ColumnConstraints();
        ColumnConstraints c2 = new ColumnConstraints();
        c2.setHgrow(Priority.ALWAYS);
        grid.getColumnConstraints().addAll(c1, c2, new ColumnConstraints());

        VBox box = new VBox(grid);
        HBox.setHgrow(box, Priority.ALWAYS);
        getChildren().add(box);
    }
}
